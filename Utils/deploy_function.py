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
"""----------------------------------------"""

# Ratelimiter
# put_function(url, token, path+'/RateLimiter/bin', 'ratelimiter.tar.gz', 'com.urv.zion.function.ratelimiter.Handler')

# Encryption
# put_function(url, token, path+'/Encryption/bin', 'encryption.tar.gz', 'com.urv.zion.function.encryption.Handler')

# UONE Reducer
# put_function(url, token, path+'/DataAggregation/Reducer/bin', 'uone-reducer.tar.gz', 'com.urv.zion.function.reducer.Handler')

# UONE Filter
# ut_function(url, token, path+'/DataAggregation/UbuntuOneFilter/bin', 'uone-filter.tar.gz', 'com.urv.zion.function.uone.Handler')

# Compression
# put_function(url, token, path+'/Compression/bin', 'compression.tar.gz', 'com.urv.zion.function.compression.Handler')

# Resizer
# put_function(url, token, path+'/ImageResizer/bin', 'image-resizer.tar.gz', 'com.urv.zion.function.imageresizer.Handler')

# Blur Faces
# put_function(url, token, path+'/BlurFaces/bin', 'blurfaces.tar.gz', 'com.urv.zion.function.blurfaces.Handler')

# Signature validator
# put_function(url, token, path+'/SignatureValidator/bin', 'signature-validator.tar.gz', 'com.urv.zion.function.signaturevalidator.Handler')

# Prefetching
# put_function(url, token, path+'/Prefetching/bin', 'prefetching.tar.gz', 'com.urv.zion.function.prefetching.Handler')

# ACCESS LIMITER
# put_function(url, token, path+'/Limiter/bin', 'access-limiter.tar.gz', 'com.urv.zion.function.limiter.Handler')

# NOOP DATA ITERATOR
# put_function(url, token, '../Function Samples/java/NoopDataIterator/bin', 'nop.tar.gz', 'com.urv.zion.function.noopdataiterator.Handler')

# CBAC
# put_function(url, token, path+'/ContentBasedAccessControl/bin', 'cbac.tar.gz', 'com.urv.zion.function.cbac.Handler')

# None
put_function(url, token, path+'/None/bin', 'none.tar.gz', 'com.urv.zion.function.none.Handler')

print(url, token)
