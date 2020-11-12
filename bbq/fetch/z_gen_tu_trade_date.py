import tushare as ts
import click


@click.command()
@click.option('--token', type=str, default='408481e156da6a5facd695e58add4d0bf705649fe0f460d03d4d6908',
              help='tushare token')
@click.option('--out_file', type=str, default='tushare_trade_date.py', help='output file')
def main(token: str, out_file: str):
    tmpl = '''from datetime import datetime

{}


# 支持参数: datetime / str, 其中 str格式为 %Y-%m-%d 或 %Y%m%d
def is_trade_date(date):
    from datetime import datetime
    date_str = ''
    if isinstance(date, datetime):
        date_str = date.strftime('%Y%m%d')
    else:
        try:
            if '-' in date:
                date_str = datetime.strptime(date, '%Y-%m-%d').strftime('%Y%m%d')
            else:
                date_str = datetime.strptime(date, '%Y%m%d').strftime('%Y%m%d')
        except:
            return False
    if date_str not in _cal_dict:
        return False

    return _cal_dict[date_str]


if __name__ == '__main__':
    print('{{}} is trade day: {{}}'.format('20201116', is_trade_date('20201116')))
    print('{{}} is trade day: {{}}'.format('2020111', is_trade_date('2020111')))
    print('{{}} is trade day: {{}}'.format('2020-10-01', is_trade_date('2020-10-01')))
    print('{{}} is trade day: {{}}'.format('2020-11-16', is_trade_date('2020-11-16')))
    print('{{}} is trade day: {{}}'.format(datetime.now(), is_trade_date(datetime.now())))
    print('{{}} is trade day: {{}}'.format({{'1': 1}}, is_trade_date({{'1': 1}})))
'''

    api = ts.pro_api(token)
    df = api.trade_cal(exchange='', fields='cal_date,is_open')
    cal_dict = ''
    is_first = True
    for _, item in df.iterrows():
        if is_first:
            is_first = False
            cal_dict = "'{}': {}".format(item['cal_date'], item['is_open'] == 1)
        else:
            cal_dict = "{}, '{}': {}".format(cal_dict, item['cal_date'], item['is_open'] == 1)
    cal_dict = '_cal_dict = {{{}}}'.format(cal_dict)

    content = tmpl.format(cal_dict)
    with open(out_file, 'w') as f:
        f.write(content)


if __name__ == '__main__':
    main()
