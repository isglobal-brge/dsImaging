// Opal Resource Forms for dsImaging.
// Two S3 variants: MinIO (simple) and AWS (with region selector).

var dsImaging_resources = [
  {
    name: "imaging-minio",
    title: "Imaging Collection (MinIO / Self-hosted S3)",
    description: "An imaging dataset in a MinIO or self-hosted S3 instance.",
    tags: ["imaging", "s3", "minio", "dsImaging"],
    parameters: {
      host: {
        type: "string",
        title: "Host",
        description: "MinIO host and port (e.g. minio:9000)",
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
        description: "MinIO access key",
        required: true
      },
      secret_key: {
        type: "string",
        title: "Secret Key",
        format: "password",
        description: "MinIO secret key",
        required: true
      }
    },
    asUrl: function(parameters) {
      return "imaging+dataset://" + parameters.host + "/" +
        parameters.bucket + "/" + parameters.collection;
    },
    asIdentity: function(credentials) { return credentials.access_key; },
    asSecret: function(credentials) { return credentials.secret_key; },
    toParameters: function(url) {
      var body = url.replace("imaging+dataset://", "").replace(/@.*$/, "");
      var parts = body.split("/");
      return {
        host: parts[0] || "",
        bucket: parts[1] || "",
        collection: parts.slice(2).join("/")
      };
    }
  },
  {
    name: "imaging-aws-s3",
    title: "Imaging Collection (AWS S3)",
    description: "An imaging dataset in Amazon S3.",
    tags: ["imaging", "s3", "aws", "dsImaging"],
    parameters: {
      region: {
        type: "string",
        title: "AWS Region",
        description: "Select the AWS region where the bucket is hosted.",
        required: true,
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
      bucket: {
        type: "string",
        title: "Bucket",
        description: "S3 bucket name",
        required: true
      },
      collection: {
        type: "string",
        title: "Collection",
        description: "Path within the bucket (e.g. datasets/lung_ct_v2)",
        required: true
      }
    },
    credentials: {
      access_key: {
        type: "string",
        title: "AWS Access Key ID",
        description: "IAM access key",
        required: true
      },
      secret_key: {
        type: "string",
        title: "AWS Secret Access Key",
        format: "password",
        description: "IAM secret key",
        required: true
      }
    },
    asUrl: function(parameters) {
      var host = "s3." + parameters.region + ".amazonaws.com";
      return "imaging+dataset://" + host + "/" +
        parameters.bucket + "/" + parameters.collection +
        "@" + parameters.region;
    },
    asIdentity: function(credentials) { return credentials.access_key; },
    asSecret: function(credentials) { return credentials.secret_key; },
    toParameters: function(url) {
      var body = url.replace("imaging+dataset://", "");
      var region = "";
      var atIdx = body.lastIndexOf("@");
      if (atIdx > 0) {
        region = body.substring(atIdx + 1);
        body = body.substring(0, atIdx);
      }
      var parts = body.split("/");
      return {
        region: region,
        bucket: parts[1] || "",
        collection: parts.slice(2).join("/")
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
