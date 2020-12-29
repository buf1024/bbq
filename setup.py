from setuptools import find_packages, setup

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
        'numpy==1.19.4',
        'pandas==1.1.4',
        'akshare==0.7.6',
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
        'sklearn==0.24.0',
        'TA-Lib==0.4.19',
        'protobuf',
    ],
    entry_points={
        'console_scripts': [
            'fundsync=bbq.cmd.fund_sync:main',
            'stocksync=bbq.cmd.stock_sync:main',
            'stockselect=bbq.cmd.stock_select:main',
            'trader=bbq.trade.trader:main'
        ]
    },
)
