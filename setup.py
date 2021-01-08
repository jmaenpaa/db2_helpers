"""A setuptools based setup module.

See:
https://packaging.python.org/guides/distributing-packages-using-setuptools/
https://github.com/pypa/sampleproject
"""

# Always prefer setuptools over distutils
from setuptools import setup, find_packages
import pathlib

here = pathlib.Path(__file__).parent.resolve()

# Get the long description from the README file
long_description = (here / 'README.md').read_text(encoding='utf-8')

# Arguments marked as "Required" below must be included for upload to PyPI.
# Fields marked as "Optional" may be commented out.

setup(
    name='db2_helpers',
    version='0.1.5',
    description='Helper functions for managing database connections',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/jmaenpaa/db2_helpers',
    author='John Maenpaa',
    author_email='johnmaenpaa@db2solutions.com',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3 :: Only',
    ],
    keywords='db2 ibm_db',
    license='MIT License',
    package_dir={'': 'src'},
    packages=find_packages(where='src'),
    python_requires='>=3.8, <4',
    install_requires=['cryptography>=3.3.1', 'ibm_db>=3.0.2', 'click>=7.1.2'],
    extras_require={  # Optional
        'dev': ['check-manifest'],
        'test': ['coverage'],
    },

    # package_data={  # Optional
    #     'db2_helpers': ['package_data.dat'],
    # },
    # data_files=[('my_data', ['data/data_file'])],  # Optional

    entry_points={
        'console_scripts': [
            'db_credentials=db_commands:db_credentials',
            'db_import=db_import_export:db_import',
            'db_export=db_import_export:db_export',
        ],
    },
    project_urls={  # Optional
        'Bug Reports': 'https://github.com/jmaenpaa/db2_helpers/issues',
        'Source': 'https://github.com/jmaenpaa/db2_helpers/',
    },
)
