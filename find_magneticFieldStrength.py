# For every session in specific Flywheel projects,
# extract magnetic field strength for MR images (stored in custom file info), when available
#
#   Assumes this field is the same across all acquisitions in a session (only grabs 1 per session)
#
#   Outputs CSV with one row per session
#
#   amf
#   September 2021

api_key='<api-key>'
valid_projects = ['DIPG','DNET','HGG','LGG','Liquid_Biopsy','Medullo'] # need to finish 'DMG.HTAN'
# valid_projects = ['ATRT','CBTTC_V2'] # need to finish 'DMG.HTAN'

#  ************** MAIN PROCESSES **************
import flywheel
import pandas as pd
import datetime

# ====== access the flywheel client for your instance ====== 
fw = flywheel.Client(api_key)

# ====== define output file name 
todays_date = datetime.datetime.now().strftime('%m-%d-%y')
output_fn = "Flywheel_magneticFieldStrength_"+todays_date+".csv"

# ====== search
# loop through projects
# for each subj/session, grab the magnetic field strength & save to output df
results=[]
for this_proj in valid_projects:
    ses_cnt=1
    proj_info = fw.projects.find('label='+this_proj) # find project id
    proj_id = proj_info[0].id
    proj_cntr = fw.get_project(proj_id)
    group_label = proj_cntr.group
    print('PROCESSING: '+this_proj)
    for session in proj_cntr.sessions.iter(): # loop through all sessions in this project
        c_id = session.subject.label
        ses_label = session.label
        session_id = session.id
        acqs = fw.get_session_acquisitions(session_id)
        MRFieldStrength=[]
        while MRFieldStrength == []:
            for acq in acqs:
                for file in acq.files:
                    if file['modality'] == 'MR': # only look at MR images
                        file_cntr = fw.get_file(file.file_id) # get first acq/file
                        try:
                            MRFieldStrength = file_cntr.info['MagneticFieldStrength']
                        except:
                            MRFieldStrength = []
            break # terminate if looped through all acquisitions
        results.append([this_proj,c_id,ses_label,MRFieldStrength])
        print(this_proj+': session '+str(ses_cnt))
        ses_cnt+=1

out_df=pd.DataFrame(results,columns=["Project","C_ID","Session",'MagneticFieldStrength'])
out_df.to_csv(output_fn,index=False)
