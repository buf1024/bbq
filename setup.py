from setuptools import find_packages, setup

setup(
    name='barbar',
    version='1.0.0',
    packages=find_packages(include=['barbar']),
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
        'pytdx',
        'Click',
        'pymongo',
        'tushare',
        'TA-Lib',
        'asyncio-nats-client'
    ],
    entry_points={
        'console_scripts': [
            'quotation=barbar.cmd.quotation:main',
            'syncstock=barbar.data.stock_sync:main',
            'syncfund=barbar.data.fund_sync:main'
        ]
    },
)
