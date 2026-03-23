var dsImaging = {
  settings: {
    "title": "Imaging Resources",
    "description": "Medical imaging datasets stored in S3/MinIO or configured in the server registry.",
    "web": "https://github.com/isglobal-brge/dsImaging",
    "categories": [
      {
        "name": "minio",
        "title": "MinIO / Self-hosted",
        "description": "Imaging collections stored in a self-hosted MinIO or S3-compatible instance."
      },
      {
        "name": "aws",
        "title": "Amazon Web Services",
        "description": "Imaging collections stored in AWS S3."
      },
      {
        "name": "registry",
        "title": "Server Registry",
        "description": "Datasets pre-configured in the server's dsImaging registry."
      }
    ],
    "types": [
      {
        "name": "imaging-minio",
        "title": "Imaging Collection - MinIO / Self-hosted S3",
        "description": "An imaging dataset in a MinIO or self-hosted S3 instance. The collection must contain a manifest.yaml.",
        "tags": ["minio"],
        "parameters": {
          "$schema": "http://json-schema.org/schema#",
          "type": "array",
          "items": [
            {
              "key": "host",
              "type": "string",
              "title": "Host",
              "description": "MinIO host and port (e.g. minio:9000)"
            },
            {
              "key": "bucket",
              "type": "string",
              "title": "Bucket",
              "description": "Bucket name (e.g. imaging-data)"
            },
            {
              "key": "collection",
              "type": "string",
              "title": "Collection",
              "description": "Path within the bucket (e.g. datasets/radiomics_e2e)"
            }
          ],
          "required": ["host", "bucket", "collection"]
        },
        "credentials": {
          "$schema": "http://json-schema.org/schema#",
          "type": "array",
          "items": [
            {
              "key": "access_key",
              "type": "string",
              "title": "Access Key",
              "description": "MinIO access key"
            },
            {
              "key": "secret_key",
              "type": "string",
              "title": "Secret Key",
              "format": "password",
              "description": "MinIO secret key"
            }
          ],
          "required": ["access_key", "secret_key"]
        }
      },
      {
        "name": "imaging-aws",
        "title": "Imaging Collection - AWS S3",
        "description": "An imaging dataset in Amazon S3.",
        "tags": ["aws"],
        "parameters": {
          "$schema": "http://json-schema.org/schema#",
          "type": "array",
          "items": [
            {
              "key": "region",
              "type": "string",
              "title": "AWS Region",
              "description": "AWS region where the bucket is hosted.",
              "enum": [
                { "key": "us-east-1",      "title": "US East (N. Virginia)" },
                { "key": "us-east-2",      "title": "US East (Ohio)" },
                { "key": "us-west-1",      "title": "US West (N. California)" },
                { "key": "us-west-2",      "title": "US West (Oregon)" },
                { "key": "eu-west-1",      "title": "EU (Ireland)" },
                { "key": "eu-west-2",      "title": "EU (London)" },
                { "key": "eu-west-3",      "title": "EU (Paris)" },
                { "key": "eu-central-1",   "title": "EU (Frankfurt)" },
                { "key": "eu-central-2",   "title": "EU (Zurich)" },
                { "key": "eu-south-1",     "title": "EU (Milan)" },
                { "key": "eu-south-2",     "title": "EU (Spain)" },
                { "key": "eu-north-1",     "title": "EU (Stockholm)" },
                { "key": "ap-northeast-1", "title": "Asia Pacific (Tokyo)" },
                { "key": "ap-northeast-2", "title": "Asia Pacific (Seoul)" },
                { "key": "ap-southeast-1", "title": "Asia Pacific (Singapore)" },
                { "key": "ap-southeast-2", "title": "Asia Pacific (Sydney)" },
                { "key": "ap-south-1",     "title": "Asia Pacific (Mumbai)" },
                { "key": "ca-central-1",   "title": "Canada (Central)" },
                { "key": "sa-east-1",      "title": "South America (Sao Paulo)" },
                { "key": "me-south-1",     "title": "Middle East (Bahrain)" },
                { "key": "af-south-1",     "title": "Africa (Cape Town)" }
              ]
            },
            {
              "key": "bucket",
              "type": "string",
              "title": "Bucket",
              "description": "S3 bucket name"
            },
            {
              "key": "collection",
              "type": "string",
              "title": "Collection",
              "description": "Path within the bucket (e.g. datasets/lung_ct_v2)"
            }
          ],
          "required": ["region", "bucket", "collection"]
        },
        "credentials": {
          "$schema": "http://json-schema.org/schema#",
          "type": "array",
          "items": [
            {
              "key": "access_key",
              "type": "string",
              "title": "AWS Access Key ID",
              "description": "IAM access key"
            },
            {
              "key": "secret_key",
              "type": "string",
              "title": "AWS Secret Access Key",
              "format": "password",
              "description": "IAM secret key"
            }
          ],
          "required": ["access_key", "secret_key"]
        }
      },
      {
        "name": "imaging-registry",
        "title": "Imaging Dataset - Server Registry",
        "description": "Reference a dataset pre-configured in the server's dsImaging registry.",
        "tags": ["registry"],
        "parameters": {
          "$schema": "http://json-schema.org/schema#",
          "type": "array",
          "items": [
            {
              "key": "dataset_id",
              "type": "string",
              "title": "Dataset ID",
              "description": "Registered identifier (e.g. radiology.chest_xray.v3)"
            }
          ],
          "required": ["dataset_id"]
        },
        "credentials": {
          "$schema": "http://json-schema.org/schema#",
          "description": "No credentials required: the dataset is configured server-side."
        }
      }
    ]
  },
  asResource: function(type, name, params, credentials) {

    var toMinioResource = function(name, params, credentials) {
      return {
        name: name,
        url: "imaging+dataset://" + params.host + "/" + params.bucket + "/" + params.collection,
        identity: credentials.access_key,
        secret: credentials.secret_key
      };
    };

    var toAwsResource = function(name, params, credentials) {
      var host = "s3." + params.region + ".amazonaws.com";
      return {
        name: name,
        url: "imaging+dataset://" + host + "/" + params.bucket + "/" + params.collection + "@" + params.region,
        identity: credentials.access_key,
        secret: credentials.secret_key
      };
    };

    var toRegistryResource = function(name, params, credentials) {
      return {
        name: name,
        url: "imaging+dataset://registry/" + params.dataset_id
      };
    };

    var factories = {
      "imaging-minio": toMinioResource,
      "imaging-aws": toAwsResource,
      "imaging-registry": toRegistryResource
    };

    if (factories[type]) {
      return factories[type](name, params, credentials);
    }
    return undefined;
  }
};
