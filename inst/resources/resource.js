// Opal Resource Forms for dsImaging.
// The user fills in simple fields; the URL is built/parsed automatically.

var dsImaging_resources = [
  {
    name: "imaging-s3-collection",
    title: "Imaging Collection (S3/MinIO)",
    description: "An imaging dataset stored in S3 or MinIO. " +
      "The collection must contain a manifest.yaml.",
    tags: ["imaging", "s3", "minio", "dsImaging"],
    parameters: {
      host: {
        type: "string",
        title: "Host",
        description: "S3/MinIO host and port (e.g. minio:9000 or s3.amazonaws.com)",
        required: true
      },
      bucket: {
        type: "string",
        title: "Bucket",
        description: "Bucket name (e.g. imaging-data)",
        required: true
      },
      collection: {
        type: "string",
        title: "Collection",
        description: "Path within the bucket (e.g. datasets/radiomics_e2e)",
        required: true
      }
    },
    credentials: {
      access_key: {
        type: "string",
        title: "Access Key",
        description: "S3/MinIO access key",
        required: true
      },
      secret_key: {
        type: "string",
        title: "Secret Key",
        format: "password",
        description: "S3/MinIO secret key",
        required: true
      }
    },
    asUrl: function(parameters) {
      // imaging+dataset://host:port/bucket/collection
      return "imaging+dataset://" + parameters.host + "/" +
        parameters.bucket + "/" + parameters.collection;
    },
    asIdentity: function(credentials) {
      return credentials.access_key;
    },
    asSecret: function(credentials) {
      return credentials.secret_key;
    },
    toParameters: function(url) {
      // Parse: imaging+dataset://host:port/bucket/collection
      var body = url.replace("imaging+dataset://", "");
      var firstSlash = body.indexOf("/");
      if (firstSlash < 0) return { host: body };
      var host = body.substring(0, firstSlash);
      var rest = body.substring(firstSlash + 1);
      var secondSlash = rest.indexOf("/");
      if (secondSlash < 0) return { host: host, bucket: rest };
      return {
        host: host,
        bucket: rest.substring(0, secondSlash),
        collection: rest.substring(secondSlash + 1)
      };
    }
  },
  {
    name: "imaging-dataset-id",
    title: "Imaging Dataset (Registry)",
    description: "Reference a dataset pre-configured in the server's dsImaging registry.",
    tags: ["imaging", "dataset", "dsImaging"],
    parameters: {
      dataset_id: {
        type: "string",
        title: "Dataset ID",
        description: "Registered identifier (e.g. radiology.chest_xray.v3)",
        required: true
      }
    },
    credentials: {},
    asUrl: function(parameters) {
      return "imaging+dataset://registry/" + parameters.dataset_id;
    },
    toParameters: function(url) {
      return { dataset_id: url.replace("imaging+dataset://registry/", "") };
    }
  }
];
