from setuptools import setup, find_packages

setup(
    name='python-server-cantrips',
    version='0.0.6',
    namespace_packages=['cantrips'],
    packages=find_packages(),
    url='https://github.com/luismasuelli/python-server-cantrips',
    license='LGPL',
    author='Luis y Anita',
    author_email='luismasuelli@hotmail.com',
    description='Python library with small server patterns (i.e. not full protocols/managements, but just small patterns) to integrate a real-time server architecture',
    install_requires=['python-cantrips>=0.7.5', 'enum34>=1.0.4', 'six>=1.9.0', 'msgpack-python==0.4.8']
)