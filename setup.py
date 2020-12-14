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
        'numpy',
        'pandas',
        'akshare',
        'opendatatools',
        'tushare',
        'aiohttp',
        'motor',
        'pymongo',
        'xlrd',
        'aiosmtplib',
        'Click',
        'PyYAML',
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
