# Module: Compatibility Aliases
#
# dsImaging is the canonical clinical imaging package. These aliases preserve
# the former radiomics-oriented server method names while new client code uses
# the imaging* names.

#' @rdname imagingCapabilitiesDS
#' @export
radiomicsCapabilitiesDS <- function() imagingCapabilitiesDS()

#' @rdname imagingInstallModelDS
#' @export
radiomicsInstallModelDS <- function(admin_key_encoded, provider, task) {
  imagingInstallModelDS(admin_key_encoded, provider, task)
}

#' @rdname imagingListModelsDS
#' @export
radiomicsListModelsDS <- function() imagingListModelsDS()

#' @rdname imagingRadiomicsScanCollectionDS
#' @export
radiomicsScanCollectionDS <- function(dataset_id_enc, segmenter_enc,
                                      profile_enc, visibility_enc) {
  imagingRadiomicsScanCollectionDS(dataset_id_enc, segmenter_enc,
    profile_enc, visibility_enc)
}

#' @rdname imagingRadiomicsSubmitBatchDS
#' @export
radiomicsSubmitBatchDS <- function(generation_id_enc, sample_ids_enc,
                                   segmenter_enc, profile_enc,
                                   dataset_id_enc, fingerprints_enc,
                                   content_hashes_enc = NULL) {
  imagingRadiomicsSubmitBatchDS(generation_id_enc, sample_ids_enc,
    segmenter_enc, profile_enc, dataset_id_enc, fingerprints_enc,
    content_hashes_enc)
}

#' @rdname imagingRadiomicsCollectionStatusDS
#' @export
radiomicsCollectionStatusDS <- function(generation_id_enc) {
  imagingRadiomicsCollectionStatusDS(generation_id_enc)
}

#' @rdname imagingRadiomicsPublishCollectionDS
#' @export
radiomicsPublishCollectionDS <- function(generation_id_enc, dataset_id_enc,
                                         allow_partial_enc) {
  imagingRadiomicsPublishCollectionDS(generation_id_enc, dataset_id_enc,
    allow_partial_enc)
}

#' @rdname imagingSegmentationValidateMasksDS
#' @export
radiomicsValidateMasksDS <- function(generation_id_enc, dataset_id) {
  imagingSegmentationValidateMasksDS(generation_id_enc, dataset_id)
}

#' @rdname imagingSegmentationGetMaskPaths
#' @export
radiomicsGetMaskPaths <- function(generation_id) {
  imagingSegmentationGetMaskPaths(generation_id)
}
