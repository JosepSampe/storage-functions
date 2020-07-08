from swiftclient import client as c
import os


def put_function(url, token, function_path, fuction_name, main):
    f = open('%s/%s' % (function_path, fuction_name), 'rb')
    content_length = os.stat(function_path+'/'+fuction_name).st_size
    response = dict()

    metadata = {'X-Object-Meta-Function-Language': 'Java',
                'X-Object-Meta-Function-Memory': 1024,
                'X-Object-Meta-Function-Timeout': 10,
                'X-Object-Meta-Function-Main': main}

    c.put_object(url, token, 'functions', fuction_name, f,
                 content_length, None, None,
                 "application/x-tar", metadata,
                 None, None, None, response)
    f.close()
    status = response.get('status')
    assert (status == 200 or status == 201)


keystone_url = "http://10.30.223.232:5000/v3"
ACCOUNT = 'zion'
USER_NAME = 'zion'
PASSWORD = 'zion'

url, token = c.get_auth(keystone_url, ACCOUNT + ":"+USER_NAME, PASSWORD, auth_version="3")

path = '../Function Samples/java'
build_path = '../Function\ Samples/java'
"""----------------------------------------"""

# Ratelimiter
# os.system('ant -f {}/RateLimiter/build.xml build'.format(build_path))
# put_function(url, token, path+'/RateLimiter/bin', 'ratelimiter.tar.gz', 'com.urv.zion.function.ratelimiter.Handler')
# os.system('rm -R {}/RateLimiter/bin'.format(build_path))

# Encryption
# os.system('ant -f {}/Encryption/build.xml build'.format(build_path))
# put_function(url, token, path+'/Encryption/bin', 'encryption.tar.gz', 'com.urv.zion.function.encryption.Handler')
# os.system('rm -R {}/Encryption/bin'.format(build_path))

# UONE Reducer
# os.system('ant -f {}/DataAggregation/Reducer/build.xml build'.format(build_path))
# put_function(url, token, path+'/DataAggregation/Reducer/bin', 'uone-reducer.tar.gz', 'com.urv.zion.function.reducer.Handler')
# os.system('rm -R {}/DataAggregation/Reducer/bin'.format(build_path))

# UONE Filter
# os.system('ant -f {}/DataAggregation/UbuntuOneFilter/build.xml build'.format(build_path))
# put_function(url, token, path+'/DataAggregation/UbuntuOneFilter/bin', 'uone-filter.tar.gz', 'com.urv.zion.function.uone.Handler')
# os.system('rm -R {}/DataAggregation/UbuntuOneFilter/bin'.format(build_path))

# Compression
# os.system('ant -f {}/Compression/build.xml build'.format(build_path))
# put_function(url, token, path+'/Compression/bin', 'compression.tar.gz', 'com.urv.zion.function.compression.Handler')
# os.system('rm -R {}/Compression/bin'.format(build_path))

# Resizer
# os.system('ant -f {}/ImageResizer/build.xml build'.format(build_path))
# put_function(url, token, path+'/ImageResizer/bin', 'image-resizer.tar.gz', 'com.urv.zion.function.imageresizer.Handler')
# os.system('rm -R {}/ImageResizer/bin'.format(build_path))

# Blur Faces
# os.system('ant -f {}/BlurFaces/build.xml build'.format(build_path))
# put_function(url, token, path+'/BlurFaces/bin', 'blurfaces.tar.gz', 'com.urv.zion.function.blurfaces.Handler')
# os.system('rm -R {}/BlurFaces/bin'.format(build_path))

# Signature validator
# os.system('ant -f {}/SignatureValidator/build.xml build'.format(build_path))
# put_function(url, token, path+'/SignatureValidator/bin', 'signature-validator.tar.gz', 'com.urv.zion.function.signaturevalidator.Handler')
# os.system('rm -R {}/SignatureValidator/bin'.format(build_path))

# Prefetching
# os.system('ant -f {}/Prefetching/build.xml build'.format(build_path))
# put_function(url, token, path+'/Prefetching/bin', 'prefetching.tar.gz', 'com.urv.zion.function.prefetching.Handler')
# os.system('rm -R {}/Prefetching/bin'.format(build_path))

# ACCESS LIMITER
# os.system('ant -f {}/Limiter/build.xml build'.format(build_path))
# put_function(url, token, path+'/Limiter/bin', 'access-limiter.tar.gz', 'com.urv.zion.function.limiter.Handler')
# os.system('rm -R {}/Limiter/bin'.format(build_path))

# COUNTER
# os.system('ant -f {}/Counter/build.xml build'.format(build_path))
# put_function(url, token, path+'/Counter/bin', 'access-counter.tar.gz', 'com.urv.zion.function.counter.Handler')
# os.system('rm -R {}/Counter/bin'.format(build_path))

# NOOP DATA ITERATOR
os.system('ant -f {}/NoopDataIterator/build.xml build'.format(build_path))
put_function(url, token, path+'/NoopDataIterator/bin', 'noop.tar.gz', 'com.urv.zion.function.noopdataiterator.Handler')
os.system('rm -R {}/NoopDataIterator/bin'.format(build_path))

# CBAC
# os.system('ant -f {}/ContentBasedAccessControl/build.xml build'.format(build_path))
# put_function(url, token, path+'/ContentBasedAccessControl/bin', 'cbac.tar.gz', 'com.urv.zion.function.cbac.Handler')
# os.system('rm -R {}/ContentBasedAccessControl/bin'.format(build_path))

# None
# os.system('ant -f {}/None/build.xml build'.format(build_path))
# put_function(url, token, path+'/None/bin', 'none.tar.gz', 'com.urv.zion.function.none.Handler')
# os.system('rm -R {}/None/bin'.format(build_path))

print(url, token)
