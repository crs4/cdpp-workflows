cwlVersion: v1.1
class: Workflow

requirements:
  InlineJavascriptRequirement: {}

inputs:
  slide:
    type: File
    secondaryFiles:
      - pattern: |-
          ${
            if (self.nameext == '.mrxs') {
              return {
              class: "Directory",
              location: self.location.match(/.*\//)[0] + "/" + self.nameroot,
              basename: self.nameroot};
            }
            else return null;
          }
        required: false
  level: int
  chunk-size: int?
  batch-size: int?
  gpu: int?

outputs:
  tissue:
    type: File
    outputSource: extract-tissue/tissue

steps:
  extract-tissue:
    run: extract_tissue.cwl
    in:
      src: slide
      level: level
      label: { default: tissue }
      gpu: gpu
      chunk-size: chunk-size
      batch-size: batch-size
    out: [tissue]

s:name: "Tissue segmentation workflow"
s:description: "Tissue segmentation workflow"
s:license: "MIT"

$namespaces:
  s: http://schema.org/
