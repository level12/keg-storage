Usage
=====

.. toctree::
   :maxdepth: 1
   :caption: Usage:


S3
--

Pre-signed URLs
^^^^^^^^^^^^^^^

The `link_to` function for the S3 backend creates a temporary, pre-signed URL that can be used for uploads or downloads.

Uploads
"""""""

- PUT request required
- Must have a header of `content-type: application/octet-stream` set
    - If header doesn't match the expected value, you will get a 400 error
- Make sure you have permissions to the key you are creating
    - The SDK will happily generate pre-signed URLs that are not available to the generating user
- Body is file contents

JavaScript example:

.. code-block:: javascript

   const resp = await axios.default.put(storageUrl, file, {
       headers: { "content-type": "application/octet-stream" },
   });
