from setuptools import find_packages, setup
import os
import shutil

dest_file = os.sep.join([os.path.expanduser('~'), '.config', 'bbq', 'config.yml'])

if not os.path.exists(dest_file):
    conf_file = os.sep.join([os.getcwd(), 'bbq', 'config', 'config.yml'])

    shutil.copy(conf_file, dest_file)

setup(
    name='bbq',
    version='0.0.1',
    packages=find_packages(include=['bbq']),
    include_package_data=True,
    zip_safe=False,
    platform="any",
    # packages=['package1', 'package2', 'package3'],
    # package_dir={
    #     'package2': 'package1',
    #     'package3': 'package1',
    # },
    install_requires=[
        'numpy',
        'pandas',
        'akshare==1.2.57',
        'opendatatools==1.0.0',
        'aiohttp==3.7.2',
        'motor',
        'pymongo==3.11.0',
        'xlrd==1.2.0',
        'aiosmtplib==1.1.4',
        'Click==7.1.2',
        'PyYAML==5.3.1',
        'pyecharts==1.9.0',
        'scipy==1.5.4',
        'sklearn',
        'TA-Lib==0.4.19',
        'nest-asyncio==1.5.1',
        'tqdm',
        'hisql',
        'pymysql',
        'requests',
        'protobuf',
        'ipython'
    ],
    entry_points={
        'console_scripts': [
            'fundsync=bbq.cmd.fund_sync:main',
            'stocksync=bbq.cmd.stock_sync:main',
            'bbqm2sql=bbq.cmd.mongo2sql_sync:main',
            'bbqsync=bbq.cmd.bbqsync:main',
            'bbqselect=bbq.cmd.stock_select:main',
            'bbqtrader=bbq.cmd.trader:main',
            'bbqmonitor=bbq.cmd.monitor:main'
        ]
    },
)
