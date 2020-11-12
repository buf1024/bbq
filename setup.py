from setuptools import find_packages, setup

setup(
    name='bbq',
    version='1.0.0',
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
        'Click',
    ],
    entry_points={
        'console_scripts': [
            'quotation=bbq.cmd.quotation:main',
            'syncstock=bbq.data.stock_sync:main',
            'syncfund=bbq.data.fund_sync:main'
        ]
    },
)
