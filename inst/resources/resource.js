// Opal Resource Forms API for imaging+dataset:// resources.
// Defines two resource form variants for the Opal admin UI.

var dsImaging_resources = [
  {
    // Variant 1: Dataset ID lookup via registry
    name: "imaging-dataset-id",
    title: "Imaging Dataset (by ID)",
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
      return { dataset_id: decodeURIComponent(body) };
    }
  },
  {
    // Variant 2: Direct manifest path
    name: "imaging-dataset-manifest",
    title: "Imaging Dataset (by Manifest)",
    description: "Reference an imaging dataset by providing the absolute path " +
      "to its manifest YAML file on the server.",
    tags: ["imaging", "dataset", "dsImaging", "manifest"],
    parameters: {
      manifest_path: {
        type: "string",
        title: "Manifest Path",
        description: "Absolute path to the dataset manifest YAML file " +
          "(e.g. /srv/datasets/chest_xray/v3/manifest.yml)",
        required: true
      }
    },
    credentials: {},
    asUrl: function(parameters) {
      return "imaging+dataset://manifest?path=" +
        encodeURIComponent(parameters.manifest_path);
    },
    toParameters: function(url) {
      var match = url.match(/\?path=(.+)$/);
      if (match) {
        return { manifest_path: decodeURIComponent(match[1]) };
      }
      return {};
    }
  }
];
