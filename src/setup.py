"""Small service for performing site or HTTP API monitoring"""

from setuptools import setup, find_packages

dependencies = [
    "aiodns==2.0.0",
    "aiohttp==3.7.3",
    "aiokafka==0.7.0",
    "async-timeout==3.0.1",
    "asyncpg==0.21.0",
    "attrs==20.3.0",
    "brotlipy==0.7.0",
    "cchardet==2.1.7",
    "certifi==2020.12.5",
    "cffi==1.14.4",
    "chardet==3.0.4",
    "idna==3.1",
    "kafka-python==2.0.2",
    "multidict==5.1.0",
    "psycopg2==2.8.6",
    "pycares==3.1.1",
    "pycparser==2.20",
    "PyYAML==5.4.1",
    "six==1.15.0",
    "sqlparse==0.4.1",
    "typing-extensions==3.7.4.3",
    "yandex-pgmigrate==1.0.6",
    "yarl==1.6.3",
    "supervisor==4.2.1",
]

setup(
    name='site-checker',
    version='0.0.1',
    author='Aleksey Romanov',
    author_email='drednout.by@gmail.com',
    url='https://github.com/drednout/site_checker',
    packages=find_packages(),
    include_package_data=True,
    install_requires=dependencies,
    scripts=['bin/site_check_http.py', 'bin/site_check_db_writer.py'],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: Other/Proprietary License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.8'
    ]
)
