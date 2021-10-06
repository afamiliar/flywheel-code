# Add metadata from JSON sidecars to NIfTI file in an acquisition
#   amf
#   Oct 2021
#
#   First uses Flywheel's dataview tool to get all file in a project & select only nifti's w/o file.info metadata.
#   Then injects metadata from JSON sidecar files into Flywheel-metdata for a given NIfTI file.
#
#   Requires:
#      -- each JSON corresponds to a NIfTI file in the same acquisition container
#      -- JSON & NIfTI have the same file names (e.g., as output by dcm2niix BIDS-sidecar option)

import flywheel
import json

api_key='<api-key>'
fw_projects = ['DMG_HTAN','DIPG']

# ====== access the flywheel client for the instance ====== 
fw = flywheel.Client(api_key)


# ====== set up data view ==========
view = fw.View(
    container="acquisition",
    filename="*",
    match="all",
    columns=[
        "file.name",
        "file.file_id",
        "file.type",
        "file.info",
    ],
    include_ids=True,
    include_labels=True,
    process_files=False,
    sort=False,
)

        # "file.created",
        # "file.modified",

# ====== loop through projects ======
path_separator='/'

for fw_proj in fw_projects:
    if fw_proj == 'CBTTC_V2':
        fw_group = 'cbttc'
    else:
        fw_group = 'd3b'
    grp_cntnr = fw.lookup(fw_group)
    proj_cntnr = grp_cntnr.projects.find_first(f'label={fw_proj}')

    df = fw.read_view_dataframe(view, proj_cntnr.id) # dataframe with all files in this proj
    df_nii = df[(df['file.type']=='nifti') & (df['file.info']=={})] # filter to nifti's with no metadata

    for index, nii_file in df_nii.iterrows(): # loop through the nifti files
        ## get acqusition
        acq_label = nii_file['acquisition.label']
        session_label = nii_file['session.label']
        subj_label = nii_file['subject.label']
        path_to_acq = path_separator.join((fw_group,fw_proj,subj_label,session_label,acq_label))
        acq = fw.lookup(path_to_acq)

        ## get nifti
        nii_cntr=[]
        nii_fn = nii_file['file.name']
        nii_cntr = acq.get_file(nii_fn)

        ## get json
        json_cntr = []
        json_fn = nii_fn.strip('.nii.gz')+'.json'
        json_cntr = acq.get_file(json_fn) # acq.files['type'=='source code']

        ## inject json metadata into nifti file info (download to temp file & load)
        if json_cntr:
            acq.download_file(json_cntr.name, 'temp.json')
            with open('temp.json') as f:
                metadata = json.load(f)
            nii_cntr.update_info(metadata)
            print('Added JSON metadata to '+subj_label+' '+session_label+' '+acq_label+' '+nii_fn)
            os.remove('temp.json')

