from setuptools import setup, find_packages

paste_factory = ['zion_handler = '
                 'zion.function_handler:filter_factory']

setup(
    name='swift-zion',
    version='0.7.0',
    description='Serverless Storage Functions Framework for OpenStack Swift',
    author='Josep Sampé',
    url='http://iostack.eu',
    packages=find_packages(),
    requires=['swift>=2.25.0', ],
    entry_points={'paste.filter_factory': paste_factory},
    classifiers=[
      'Development Status :: 5 - Production/Stable',
      'Intended Audience :: Developers',
      'Natural Language :: English',
      'License :: OSI Approved :: Apache Software License',
      'Programming Language :: Python',
      'Programming Language :: Python :: 3',
      'Programming Language :: Python :: 3.5',
      'Programming Language :: Python :: 3.6',
      'Programming Language :: Python :: 3.7',
      'Programming Language :: Python :: 3.8',
    ],
    python_requires='>=3.5',
)
