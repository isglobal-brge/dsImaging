// Opal Resource Forms API for dsImaging resources.
// Defines resource form variants for the Opal admin UI.

var dsImaging_resources = [
  {
    // Variant 1: S3/MinIO collection (recommended)
    name: "imaging-s3-collection",
    title: "Imaging Collection (S3/MinIO)",
    description: "Reference an imaging collection stored in S3 or MinIO. " +
      "The collection must have a manifest.yaml at the root.",
    tags: ["imaging", "s3", "minio", "dsImaging"],
    parameters: {
      endpoint: {
        type: "string",
        title: "S3 Endpoint",
        description: "MinIO or S3 endpoint URL (e.g. http://minio:9000)",
        required: true
      },
      bucket: {
        type: "string",
        title: "Bucket",
        description: "S3 bucket name (e.g. imaging-data)",
        required: true
      },
      collection: {
        type: "string",
        title: "Collection Path",
        description: "Path prefix within the bucket (e.g. datasets/radiomics_e2e)",
        required: true
      },
      region: {
        type: "string",
        title: "Region",
        description: "AWS region (leave empty for MinIO)",
        required: false
      }
    },
    credentials: {
      access_key: {
        type: "string",
        title: "Access Key",
        description: "S3/MinIO access key ID",
        required: true
      },
      secret_key: {
        type: "string",
        title: "Secret Key",
        format: "password",
        description: "S3/MinIO secret access key",
        required: true
      }
    },
    asUrl: function(parameters, credentials) {
      // Build: imaging+dataset://s3/{bucket}/{collection}?endpoint=...
      var url = "imaging+dataset://s3/" +
        encodeURIComponent(parameters.bucket) + "/" +
        parameters.collection;
      var query = "?endpoint=" + encodeURIComponent(parameters.endpoint);
      if (parameters.region) {
        query += "&region=" + encodeURIComponent(parameters.region);
      }
      return url + query;
    },
    asIdentity: function(credentials) {
      return credentials.access_key;
    },
    asSecret: function(credentials) {
      return credentials.secret_key;
    },
    toParameters: function(url) {
      // Parse: imaging+dataset://s3/{bucket}/{collection}?endpoint=...
      var match = url.match(/imaging\+dataset:\/\/s3\/([^/]+)\/([^?]+)/);
      var params = {};
      if (match) {
        params.bucket = decodeURIComponent(match[1]);
        params.collection = match[2];
      }
      var ep = url.match(/endpoint=([^&]+)/);
      if (ep) params.endpoint = decodeURIComponent(ep[1]);
      var rg = url.match(/region=([^&]+)/);
      if (rg) params.region = decodeURIComponent(rg[1]);
      return params;
    }
  },
  {
    // Variant 2: Dataset ID lookup via server registry
    name: "imaging-dataset-id",
    title: "Imaging Dataset (by Registry ID)",
    description: "Reference an imaging dataset by its registered dataset ID. " +
      "The dataset must be configured in the server's dsImaging registry.",
    tags: ["imaging", "dataset", "dsImaging"],
    parameters: {
      dataset_id: {
        type: "string",
        title: "Dataset ID",
        description: "Registered dataset identifier (e.g. radiology.chest_xray.v3)",
        required: true
      }
    },
    credentials: {},
    asUrl: function(parameters) {
      return "imaging+dataset://" + encodeURIComponent(parameters.dataset_id);
    },
    toParameters: function(url) {
      var body = url.replace("imaging+dataset://", "");
      var idx = body.indexOf("?");
      if (idx >= 0) body = body.substring(0, idx);
      return { dataset_id: decodeURIComponent(body) };
    }
  }
];
