import subprocess as sub
import click


@click.command()
@click.option('--sec', default=5, type=int, help='wait seconds')
def main(sec=5):
    fundp = sub.Popen(['fundsync'])
    stockp = sub.Popen(['stocksync', '--no-skip-basic'])
    while fundp.returncode is None or stockp.returncode is None:
        try:
            if fundp.returncode is None:
                fundp.wait(sec)
            if stockp.returncode is None:
                stockp.wait(sec)
        except sub.TimeoutExpired:
            continue
    m2sqlp = sub.Popen(['bbqm2sql'])
    while m2sqlp.returncode is None:
        try:
            m2sqlp.wait(sec)
        except sub.TimeoutExpired:
            continue
    print('done')


if __name__ == '__main__':
    main()
