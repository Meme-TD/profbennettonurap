import pandas as pd
import glob
import os

#dont forget to change file path
source_folder = "/Users/marcusling/Downloads/FFIEC_Data"
output_excel = "FFIEC_2001_2025_Summary.xlsx"
start_year = 2001
end_year = 2025

ordered_variables = [
    'RCFD2170',
    'RCON3387', 'RCFD3368',
    'RCON1763',
    'RCON5570', 'RCON5571', 'RCON5572', 'RCON5573', 'RCON5574', 'RCON5575'
]

dictionary = {
    'Schedule RC ':   ['RCFD2170'],
    'Schedule RCK':   ['RCON3387', 'RCFD3368'],
    'Schedule RCCI':  ['RCON1763'],
    'Schedule RCCII': ['RCON5570', 'RCON5571', 'RCON5572', 'RCON5573', 'RCON5574', 'RCON5575']
}
all_data_frames = []

for year in range(start_year, end_year + 1):
    year_df = None
    for schedule_name, variables_to_find in dictionary.items():
        search = os.path.join(source_folder, "**", f"*{schedule_name}*{year}*.txt")
        found_files = glob.glob(search, recursive=True)

        if not found_files:
            continue
        file_path = found_files[0] #use first match found

        try:
            df = pd.read_csv(file_path, sep='\t', header=0, low_memory=False) #tab seperated
            df = df.iloc[1:] #row 1 (index 0 because headers) is descriptions

            #idrssd --> numbers --> integers
            df['IDRSSD'] = pd.to_numeric(df['IDRSSD'], errors='coerce') #coerce turns bad stuff into NaN
            df = df.dropna(subset=['IDRSSD']) #disregard NaN rows
            df['IDRSSD'] = df['IDRSSD'].astype(int)

            #keep relevant columns
            cols_to_keep = ['IDRSSD'] + list(set(variables_to_find).intersection(df.columns))
            df = df[cols_to_keep]

            for col in cols_to_keep:
                if col != 'IDRSSD':
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

            #merge
            if year_df is None: #first file becomes base
                year_df = df
            else:
                year_df = pd.merge(year_df, df, on='IDRSSD', how='outer') #outer join keeps banks even if they are missing one schedule

        except Exception as e:
            print(f"Error could not read {schedule_name}: {e}")

    #create column of year numbers
    if year_df is not None:
        year_df['Year'] = year
        all_data_frames.append(year_df)

#export to excel
if all_data_frames:
    final_df = pd.concat(all_data_frames, ignore_index=True) #stack all years vertically into one file
    final_cols = ['Year', 'IDRSSD'] + ordered_variables
    final_df = final_df.reindex(columns=final_cols).fillna(0) #fills in missing columns (missing variables)

    final_df.to_excel(os.path.join(os.path.expanduser("~"), "Desktop", output_excel), index=False)
    print(f"Success!!! file saved to desktop as: {output_excel}")
else:
    print("Failure, check your folder path :(")