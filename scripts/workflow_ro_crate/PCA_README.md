# Prostate cancer classification workflow

This workflow segments tissue regions and classifies prostate cancer on H&E whole slide images, using AI. It consists of three steps:

1. low-resolution tissue segmentation to select areas for further processing;

2. high-resolution tissue segmentation to refine borders - it uses step 1 as input;

3. high-resolution normal/cancer classification - it uses step 1 as input.
