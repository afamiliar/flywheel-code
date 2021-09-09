## Use de-id export gear to copy existing data on Flywheel, specified by CSV input
#   amf
#   Aug 2021
#
#       Source project must have deid_profil_blank.yaml attached

import backoff
import flywheel
import csv
import sys
import pandas as pd

dest_proj = 'CBTN_D0143_staging'
target_fw_group = 'd3b'
input_fn = 'Flywheel_existing_sessions_2021-09-08.csv'

# ====== access the flywheel client for your instance ====== 
fw = flywheel.Client()

# ====== load subj lists ====== 
# input_file = csv.DictReader(open(input_fn, "r", encoding="utf-8-sig"))
input_file = pd.read_csv(input_fn)

# ====== get the gear ====== 
deid_export_gear = fw.gears.find_first('gear.name=deid-export')

# helper function
@backoff.on_exception(backoff.expo, flywheel.rest.ApiException, max_time=300)
def launch_gear(gear, inputs, config=None, destination=None):
    return gear.run(inputs=inputs, config=config, destination=destination)

count = 1

project_list=input_file.Project.unique()

# ===== make sure each source project has a de-id profile before proceeding
for source_proj in project_list:
    if not source_proj == 'CBTN_D0143':
        if source_proj == 'CBTTC_V2':
            fw_group = 'cbttc'
        else:
            fw_group = 'd3b'
        path_separator='/'
        deid_template = fw.lookup(path_separator.join((fw_group,source_proj))).get_file('deid_profile_blank.yaml')
        if not deid_template:
            print('Error finding specified de-id profile in project '+source_proj)
            sys.exit(1) # exiing with a non zero value is better for returning from an error
print('Success! Found de-id templates in source project(s).')

# ===== run the gear for every specified session 
# for row in input_file: # for CSV reader method
for index, row in input_file.iterrows():
    sub = row["C_ID"]
    ses = row["Session"]
    source_proj = row["Project"]

    if not source_proj == 'CBTN_D0143':
        if source_proj == 'CBTTC_V2':
            fw_group = 'cbttc'
        else:
            fw_group = 'd3b'

        # ===== Get deid profile file from source project
        path_separator='/'
        deid_template = fw.lookup(path_separator.join((fw_group,source_proj))).get_file('deid_profile_blank.yaml')
        if not deid_template:
            print('Error finding specified de-id profile in project '+source_proj)
            sys.exit(1) # exiing with a non zero value is better for returning from an error

        # ===== Set inputs and configuration
        inputs = {'deid_profile': deid_template}
        config = {
            'debug': False,
            'overwrite_files': False,
            'project_path': path_separator.join((target_fw_group,dest_proj)) # Destination project
        }

        # ===== SESSION-LEVEL: copy specific subj/sessions from source to target project
        try:
            ses_cntr = fw.lookup(path_separator.join((fw_group,source_proj,sub,ses)))
            if not ses_cntr:
                print(sub+' '+ses+' not found in project '+source_proj)
                continue
            session = fw.get_session(ses_cntr.id)
            launch_gear(deid_export_gear, inputs, config=config, destination=session)
            print('Copying '+sub+' '+ses+' from '+source_proj+' to '+dest_proj)
        except:
            print('ERROR finding '+sub+' '+ses+' in '+source_proj+' project')
            continue

        count+=1

        # ===== SUBJECT-LEVEL: copy any/all data for this subject across Flywheel projects (doesn't account for duplicate sessions across projects)
        #  loop through projects, find matches & launch the gear for each
        # all_fw_projects = fw.projects()
        # for proj in all_fw_projects:
        #     if proj.label == source_proj:
        #         try:
        #             sub_cntr = fw.lookup(path_separator.join((fw_group,proj.label,sub)))
        #             if not sub_cntr:
        #                 print('Subject '+sub+' not found in project '+proj.label)
        #                 continue
        #             subject = fw.get_subject(sub_cntr.id)
        #             launch_gear(deid_export_gear, inputs, config=config, destination=subject)
        #             print('Copying subject '+sub+' from '+proj.label+' project')
        #         except:
        #             continue
