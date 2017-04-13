from setuptools import setup, find_packages

paste_factory = ['synchronous_functions = '
                 'swift_middleware.function_handler:filter_factory']

setup(name='swift-blackeagle',
      version='0.1.0',
      description='Synchronous functions middleware for OpenStack Swift',
      author='Josep Sampe',
      url='http://iostack.eu',
      packages=find_packages(),
      requires=['swift(>=1.4)'],
      entry_points={'paste.filter_factory': paste_factory}
      )
