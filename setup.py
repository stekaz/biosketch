from setuptools import setup, find_packages
from versioneer import get_version, get_cmdclass

with open('README.md') as f:
    long_description = f.read()

with open('LICENSE') as f:
    license = f.read()

with open('requirements.txt') as f:
    requirements = f.read()

setup(
    name='biosketch',
    version=get_version(),
    cmdclass=get_cmdclass(),
    author='Stephen Kazakoff',
    author_email='stephen.kazakoff@qimrberghofer.edu.au',
    url='https://github.com/stekaz/biosketch',
    description='Estimate sequence similarity using probabilistic data structures',
    long_description=long_description,
    long_description_content_type='text/markdown',
    license=license,
    install_requires=requirements,
    package_dir={'': 'src'},
    packages=find_packages('src'),
    python_requires='>=3.9',
    entry_points={
        'console_scripts': [
            'biosketch=biosketch.__main__:safe_entry_point',
        ]
    },
)
