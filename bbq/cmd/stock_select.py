import yaml
import click
from bbq.common import run_until_complete
from bbq.common import setup_db, setup_log
from bbq.data.funddb import FundDB
from bbq.data.stockdb import StockDB
from bbq.selector.strategy import strategies
import base64
from bbq.config import init_def_config


async def select_async(js, config):
    ctx = config['ctx']
    strategy = config['strategy']
    count = config['count']
    regression = config['regression']

    cls_inst = strategies[strategy](db=ctx.obj['db'])
    if not await cls_inst.init(**js):
        print('strategy init failed')
        return

    codes = await cls_inst.select()
    if codes is not None:
        if len(codes) > count:
            codes = codes[:count]
        print('select codes:\n  {}'.format(', '.join(codes)))
        if regression:
            result = await cls_inst.regression(codes)
            print('select regression result:\n{}'.format(result))
    if codes is None:
        print('no code selected')
    await cls_inst.destroy()


@click.group()
@click.pass_context
@click.option('--uri', type=str, default='mongodb://localhost:27017/',
              help='mongodb connection uri, default: mongodb://localhost:27017/')
@click.option('--pool', default=10, type=int, help='mongodb connection pool size, default: 10')
@click.option('--syn_type', default='stock', type=str, help='selector type, default: stock')
@click.option('--debug/--no-debug', default=True, type=bool, help='show debug log, default: --debug')
def main(ctx, uri: str, pool: int, syn_type: str, debug: bool):
    ctx.ensure_object(dict)
    _, conf_dict = init_def_config()
    conf_dict['mongo'].update(dict(uri=uri, pool=pool))
    conf_dict['log'].update(dict(level="debug" if debug else "critical"))
    logger = setup_log(conf_dict, 'select.log')
    cls = StockDB
    if syn_type != 'stock':
        cls = FundDB
    db = setup_db(conf_dict, cls)
    if db is None:
        return

    ctx.obj['db'] = db
    ctx.obj['logger'] = logger


@main.command()
@click.pass_context
@click.option('--strategy', type=str, help='strategy full name')
def help(ctx, strategy: str):
    if strategy is None or len(strategy) == '':
        print('please provide strategy name, via --strategy argument')
        return

    names = strategies.keys()
    if strategy not in names:
        print('strategy "{}", not found, available names: \n  {}'.format(strategy, '  \n'.join(names)))
        return

    cls_inst = strategies[strategy](db=ctx.obj['db'])
    print('strategy {}:\n{}'.format(strategy, cls_inst.desc()))


@main.command()
@click.pass_context
def list(ctx):
    print('strategies: ')
    for strategy in strategies.keys():
        print('  {}'.format(strategy))


@main.command()
@click.pass_context
@click.option('--strategy', type=str, help='strategy name')
@click.option('--argument', type=str, help='strategy argument, yml string/base64 yml string')
@click.option('--count', type=int, help='select count, default 10')
@click.option('--regression/--no-regression', type=bool, help='auto regression, default False')
def select(ctx, strategy: str, argument: str, count: int, regression: bool):
    count = 10 if count is None else count
    regression = False if regression is None else regression

    names = strategies.keys()
    if strategy not in names:
        print('strategy "{}", not found, available names: \n  {}'.format(strategy, '  \n'.join(names)))
        return
    js = {}
    if argument is not None and len(argument) != 0:
        for i in range(2):
            try:
                if i != 0:
                    argument = base64.b64decode(str.encode(argument, encoding='utf-8'))

                js = yaml.load(argument, yaml.FullLoader)
            except Exception as e:
                if i != 0:
                    print('argument is not legal yml string/base64 encode yml string, please check --argument')
                    return
    config = dict(ctx=ctx, strategy=strategy, count=count, regression=regression)
    run_until_complete(select_async(js=js, config=config))


if __name__ == '__main__':
    run_until_complete(main())
