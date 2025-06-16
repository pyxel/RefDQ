"""
RefDQ MVP version 0.01
© 2025 Mark Sabin <morboagrees@gmail.com>
Released under Apache 2.0 license. Please see https://github.com/pyxel/RefDQ/blob/main/LICENSE.
"""

import streamlit as st
import pandas as pd
from refdata import RefData, Target, CheckResult, get_target_table_names, get_target_sample

max_error_rows = 10000

st.logo('logo.png')
st.title('RefDQ')

col1, col2 = st.columns([.87, .13])
col1.write("Data quality gateway for reference data.")
# Reset button
reset = col2.button(label = "Reset")

# Initialise variables
target_table_name = None
do_upload = False
if 'current_step' not in st.session_state or reset:
    st.session_state.current_step = 1
if 'continue_on_schema_error' not in st.session_state or reset:
    st.session_state.continue_on_schema_error = False
if 'rd' not in st.session_state or reset:
    st.session_state.rd = None

if reset: st.rerun()

##############################
## Upload a file
##############################

st.subheader("Upload a file")
uploaded_file = st.file_uploader(label = "")
if int(st.session_state.current_step) >= 1:
    upload_df = None
    if uploaded_file is not None:
        upload_df = pd.read_csv(uploaded_file)
        upload_df.columns = [c.upper() for c in upload_df.columns]
        st.write("Sample rows from file:")
        st.dataframe(data = upload_df.head(100), hide_index = True)
        if int(st.session_state.current_step) == 1:
            st.session_state.current_step = 2


if int(st.session_state.current_step) >= 2:
    target_table_names = get_target_table_names()
    st.subheader("Select a table to write to")
    upload_type_select = st.selectbox(label = "Upload type", options = ['Merge (upsert)', 'Replace'])
    upload_type = 'replace' if upload_type_select == 'Replace' else 'merge'

    target_table_name = st.selectbox(label = "Table", options = target_table_names, index = None)

    if target_table_name:
        st.write("Sample rows from table:")
        table_sample_df = get_target_sample(target_table = target_table_name)
        st.dataframe(data = table_sample_df, hide_index = True)

if int(st.session_state.current_step) == 2:
    st.session_state.current_step = 3 if upload_df is not None and target_table_name is not None else 2

##############################
## Create the RefData object
##############################
if int(st.session_state.current_step) == 3:
    with st.spinner("Running checks.."):
        st.session_state.rd = RefData(
            target = Target(target_table_name = target_table_name),
            df = upload_df,
            upload_type = upload_type,
            ignore_schema_error = st.session_state.continue_on_schema_error
        )

##############################
## Schema check
##############################
def click_continue_on_schema_error():
    st.session_state.continue_on_schema_error = True

if int(st.session_state.current_step) >= 3:
    st.subheader("Check schema")
    st.write("Schema")
    if st.session_state.rd.diffschema == []:
        st.info('Schema matches!', icon="✅")
        st.session_state.current_step = 4
    else:
        st.error('Schema of uploaded file differs to that of the target table.', icon="❌")
        st.dataframe(pd.DataFrame(
            st.session_state.rd.diffschema,
            columns = ['Column name', 'Table Data type', 'Uploaded Data type', 'Error']),
            hide_index = True,
            use_container_width = True
        )

        if st.session_state.continue_on_schema_error:
            st.session_state.current_step = 4
            st.warning('Ignoring schema errors.', icon='⚠️')
        else:
            st.button(
                'Ignore and continue',
                key = 'btn_continue_on_schema_error',
                type = 'primary',
                icon = ":material/warning_off:",
                on_click = click_continue_on_schema_error
            )
        
        
##############################
## DQ checks
##############################
if st.session_state.current_step >= 4:
    st.subheader('Impact')
    st.write(f'This upload will:')
    col1, col2 = st.columns([.5, .5])
    changes = False
    if upload_type == 'merge':
        col1.info(f"Insert {st.session_state.rd.impact['inserted']} rows.", icon=":material/add_circle:")
        col2.info(f"Update {st.session_state.rd.impact['updated']} rows.", icon=":material/sync:")
        changes = st.session_state.rd.impact['merge_rows_affected'] > 0
    else:
        col1.info(f"Delete {st.session_state.rd.impact['table_rows']} rows currently in the table.", icon=":material/delete_forever:")
        col2.info(f"Insert {st.session_state.rd.impact['upload_rows']} rows from the uploaded file.", icon=":material/add_circle:")
        changes = True
        
    if not changes:
        st.info('No changes to be uploaded. Data in file matches data already in table.', icon='⚠️')
    if int(st.session_state.current_step) == 4 and changes:
            st.session_state.current_step = 5

if st.session_state.current_step >= 5:
    st.subheader("Data quality checks")
    # Display DQ checks
    for check_result in st.session_state.rd.check_results:
        if len(check_result.df) > 0:
            exp = st.expander(f"❌ **{check_result.check_type}**") 
            exp.write(f"{check_result.description}")
            exp.info('Failed!', icon="❌")
            max_error_message = f"(first {max_error_rows} rows of {len(check_result.df)})" if len(check_result.df) > max_error_rows else ""
            exp.write(f"The **{check_result.check_type}** check has failed for the rows below{max_error_message}. Please correct the source file and upload again.")
            #st.badge("Failed", icon=":material/dangerous:", color="red")
            exp.dataframe(data = check_result.df[:max_error_rows], hide_index = True)
        else:
            exp = st.expander(f"✅ **{check_result.check_type}**")
            exp.write(f"{check_result.description}")
            exp.info('Passed!', icon="✅")
            exp.write(f"The **{check_result.check_type}** check has passed for all rows.")
            #st.badge("Passed", icon=":material/check:", color="green")

        if int(st.session_state.current_step) == 5 and st.session_state.rd.all_checks_passed:
            st.session_state.current_step = 6

        #st.divider()
 

##############################
## Upload data prompt
##############################
if int(st.session_state.current_step) >= 6:
    st.subheader("Upload data")
    # Upload button
    do_upload = st.button(label = "Upload", icon = ":material/upload_file:")
    if int(st.session_state.current_step) == 6:
        st.session_state.current_step = 7


##############################
## Upload data
##############################
if do_upload and int(st.session_state.current_step) == 7:
    with st.spinner("Uploading data..."):
        st.session_state.rd.upload_data()
    st.write("Done!")
