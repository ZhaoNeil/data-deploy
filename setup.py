import os
import setuptools
import shutil


def read(fname):
    path = os.path.join(os.path.dirname(__file__), fname)
    f = open(path)
    return f.read()

install_requires = [x for x in read('requirements.txt').strip().split('\n') if x]

setuptools.setup(
    name='data_deploy',
    version='0.5.0',
    author='Sebastiaan Alvarez Rodriguez',
    author_email='a@b.c',
    description='Data deployment Plugin, using metareserve reservation system',
    long_description=read('README.md'),
    long_description_content_type='text/markdown',
    url='https://github.com/Sebastiaan-Alvarez-Rodriguez/data-deploy',
    packages=setuptools.find_packages(),
    package_dir={'': '.'},
    classifiers=(
        'Environment :: Console',
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ),
    install_requires=install_requires,
    entry_points={
        'console_scripts': [
            'data-deploy = data_deploy.cli.entrypoint:main',
            ],
    },
)