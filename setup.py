from setuptools import setup, find_packages
import os

# Read the requirements.txt file


def read_requirements():
    with open('requirements.txt') as f:
        return f.read().splitlines()


def run_main_script():
    os.system('python weather_api/etl_process.py')


setup(
    name='WeatherApi',
    version='0.1',
    packages=find_packages(),
    install_requires=read_requirements(),
    entry_points={
        'console_scripts': [
            'run-main=weather_api:run_main_script',
        ],
    },
)
