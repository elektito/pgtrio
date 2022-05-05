from setuptools import setup


with open('pgtrio/_version.py') as f:
    exec(f.read())

test_dependencies = [
    'pytest==7.1.1',
    'pytest-trio==0.7.0',
]

dev_dependencies = test_dependencies + [
    'wheel',
    'twine==4.0.0',
]

setup(
    name='pgtrio',
    version=__version__,
    description='A Trio-Native PostgreSQL Interface Library',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='http://github.com/elektito/pgtrio',
    author='Mostafa Razavi',
    author_email='mostafa@sepent.com',
    license='MIT',
    packages=['pgtrio'],
    zip_safe=False,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        'trio==0.20.0',
        'python-dateutil==2.8.2',
        'orjson==3.6.8',
    ],
    extras_require={
        'test': test_dependencies,
        'dev': dev_dependencies,
    },
)
