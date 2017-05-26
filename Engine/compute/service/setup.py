from setuptools import setup, find_packages

paste_factory = ['app=middlebox.application.server:app_factory']

setup(name='MiddleBox',
      version='0.0.1',
      description='WSGI application for Compute nodes.',
      author='Daniel Barcelona',
      packages=find_packages(),
      requires=['PasteDeploy(>=1.3.3)', 'swift(>=1.4)'],
      entry_points={'paste.app_factory': paste_factory}
      )
