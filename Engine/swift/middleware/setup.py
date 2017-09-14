from setuptools import setup, find_packages

paste_factory = ['synchronous_functions = '
                 'zion.function_handler:filter_factory']

setup(name='swift-zion',
      version='0.6.3',
      description='Synchronous functions middleware for OpenStack Swift',
      author='Josep Sampe',
      url='http://iostack.eu',
      packages=find_packages(),
      requires=['swift(>=1.4)'],
      entry_points={'paste.filter_factory': paste_factory}
      )
