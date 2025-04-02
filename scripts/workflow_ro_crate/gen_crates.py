# Copyright (c) 2025 CRS4
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

"""\
Generate a Workflow RO-Crate for each CWL workflow.
"""

import argparse
from pathlib import Path
from urllib.parse import urlsplit

from cwl_utils.parser import load_document_by_uri
from rocrate.rocrate import ROCrate
from rocrate.model import ContextEntity, Person

THIS_DIR = Path(__file__).absolute().parent
WF_DIR = THIS_DIR.parent.parent / "cwl"
PCA_WF_PATH = WF_DIR / "pca_classification_workflow.cwl"
TISSUE_WF_PATH = WF_DIR / "tissue_segmentation_workflow.cwl"
PCA_WF_URL = "https://github.com/crs4/deephealth-pipelines"
TISSUE_WF_URL = "https://github.com/crs4/cdpp-workflows"
WROC_PROFILE_BASE_URL = "https://w3id.org/workflowhub/workflow-ro-crate"
WROC_PROFILE_VERSION = "1.0"
PCA_README = THIS_DIR / "PCA_README.md"
TISSUE_README = THIS_DIR / "TISSUE_README.md"
AUTHOR_NAME = "Mauro Del Rio"
AUTHOR_ID = "https://orcid.org/0000-0003-4934-128X"


def add_profile(crate):
    wroc_profile_id = f"{WROC_PROFILE_BASE_URL}/{WROC_PROFILE_VERSION}"
    profile = crate.add(ContextEntity(crate, wroc_profile_id, properties={
        "@type": "CreativeWork",
        "name": "Workflow RO-Crate",
        "version": WROC_PROFILE_VERSION,
    }))
    crate.root_dataset["conformsTo"] = profile


def add_author(crate):
    author = crate.add(Person(crate, AUTHOR_ID, properties={
        "name": AUTHOR_NAME
    }))
    crate.root_dataset["author"] = author


def add_readme(crate, readme_path):
    readme = crate.add_file(readme_path, dest_path="README.md")
    readme["about"] = crate.root_dataset
    readme["encodingFormat"] = "text/markdown"


def add_tools(crate, wf_def):
    for step in wf_def.steps:
        tool_path = Path(urlsplit(step.run).path)
        crate.add_file(tool_path, properties={
            "@type": ["File", "SoftwareSourceCode"],
            "name": f"{tool_path.stem} CWL tool"
        })


def make_crate(wf_path, readme_path, wf_url, out_dir, zipped=False):
    crate = ROCrate(gen_preview=False)
    add_profile(crate)
    wf_def = load_document_by_uri(wf_path, load_all=True)
    workflow = crate.add_workflow(
        wf_path, main=True, lang="cwl", lang_version=wf_def.cwlVersion,
        gen_cwl=False
    )
    annot = wf_def.extension_fields
    name = annot.get("http://schema.org/name", "CWL workflow")
    description = annot.get("http://schema.org/description", "CWL workflow")
    license = annot.get("http://schema.org/license", "MIT")
    workflow["name"] = crate.root_dataset["name"] = name
    crate.root_dataset["description"] = description
    workflow["url"] = crate.root_dataset["isBasedOn"] = wf_url
    crate.root_dataset["license"] = license
    add_author(crate)
    add_readme(crate, readme_path)
    add_tools(crate, wf_def)
    fn_tag = wf_path.stem.rsplit("_", 1)[0]
    if zipped:
        crate.write_zip((out_dir / fn_tag).with_suffix(".crate.zip"))
    else:
        crate.write(out_dir / fn_tag)


def main(args):
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    make_crate(PCA_WF_PATH, PCA_README, PCA_WF_URL, out_dir, zipped=args.zip)
    make_crate(TISSUE_WF_PATH, TISSUE_README, TISSUE_WF_URL, out_dir, zipped=args.zip)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("-z", "--zip", action="store_true",
                        help="generate zipped crates")
    parser.add_argument("-o", "--out-dir", metavar="STRING", default="crates",
                        help="output directory for the RO-Crates")
    main(parser.parse_args())
