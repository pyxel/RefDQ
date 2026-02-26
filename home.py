#RefDQ MVP version 0.4.0
#© 2025 Mark Sabin <morboagrees@gmail.com>
#Released under Apache 2.0 license. Please see https://github.com/pyxel/RefDQ/blob/main/LICENSE.

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum, auto
import os

import pandas as pd
import streamlit as st

from refdata import RefData, Target, get_target_group_names, get_target_table_names, get_target_sample, get_targets
import get_snowflake as sf


MAX_ERROR_ROWS = 10000


class Step(Enum):
    """Named steps in the upload workflow."""
    UPLOAD_FILE = auto()
    SELECT_TABLE = auto()
    VALIDATE = auto()
    SCHEMA_CHECK = auto()
    DATATYPE_CHECK = auto()
    IMPACT = auto()
    DQ_CHECKS = auto()
    UPLOAD_PROMPT = auto()
    COMPLETE = auto()


@dataclass
class AppState:
    """Centralized application state."""
    upload_df: pd.DataFrame | None = None
    uploaded_file_name: str | None = None
    target_table_name: str | None = None
    target_group_name: str | None = None
    upload_type: str = 'merge'
    continue_on_schema_error: bool = False
    rd: RefData | None = None
    auto_detected_table: str | None = None
    auto_detected_group: str | None = None
    has_changes: bool = False
    upload_complete: bool = False
    action_run: bool = False

    # Cached data
    target_group_names: list[str] = field(default_factory=list)
    target_table_names: list[str] = field(default_factory=list)

    def reset(self):
        """Reset to initial state."""
        self.upload_df = None
        self.uploaded_file_name = None
        self.target_table_name = None
        self.target_group_name = None
        self.upload_type = 'merge'
        self.continue_on_schema_error = False
        self.rd = None
        self.auto_detected_table = None
        self.auto_detected_group = None
        self.has_changes = False
        self.upload_complete = False
        self.action_run = False
        self.target_group_names = get_target_group_names()
        self.target_table_names = get_target_table_names()

    @classmethod
    def from_session(cls) -> 'AppState':
        """Load from st.session_state or create new."""
        if 'app_state' not in st.session_state:
            state = cls()
            state.target_group_names = get_target_group_names()
            state.target_table_names = get_target_table_names()
            st.session_state.app_state = state
        return st.session_state.app_state

    def save_to_session(self):
        """Persist to st.session_state."""
        st.session_state.app_state = self


class Section(ABC):
    """Base class for UI sections."""

    def __init__(self, state: AppState):
        self.state = state

    @abstractmethod
    def render(self) -> bool:
        """Render the section. Returns True if section is complete."""
        pass


def format_newline(s: str) -> str:
    """st.write needs two whitespace characters in front of a newline or it is ignored."""
    return s.replace('\n', '  \n')


def find_matching_table(upload_columns: list[str]) -> dict | None:
    """Find the table that matches the uploaded file's columns."""
    targets = get_targets()
    matches = []

    upload_cols_set = set(upload_columns)

    for target_name, target_config in targets.items():
        try:
            table_name = target_config['target_table']
            table_schema = sf.get_table_schema(table_name)
            table_cols_set = set(table_schema.keys())

            if upload_cols_set == table_cols_set:
                matches.append({
                    'table': table_name,
                    'group': target_config.get('group'),
                    'match_type': 'exact'
                })
            elif upload_cols_set.issubset(table_cols_set):
                matches.append({
                    'table': table_name,
                    'group': target_config.get('group'),
                    'match_type': 'subset',
                    'missing_cols': len(table_cols_set - upload_cols_set)
                })
        except Exception:
            continue

    if matches:
        exact_matches = [m for m in matches if m['match_type'] == 'exact']
        if exact_matches:
            return exact_matches[0]
        else:
            matches.sort(key=lambda x: x['missing_cols'])
            return matches[0]

    return None


class FileUploadSection(Section):
    """Section 1: File upload."""

    def render(self) -> bool:
        st.subheader("Upload a file")
        uploaded_file = st.file_uploader(label="")

        if uploaded_file is None:
            return False

        # Parse file
        if os.path.splitext(uploaded_file.name)[1] == '.xlsx':
            upload_df = pd.read_excel(uploaded_file, keep_default_na=False, na_values=[''], dtype=str)
        else:
            upload_df = pd.read_csv(uploaded_file, keep_default_na=False, na_values=[''], dtype=str)
        upload_df.columns = [c.upper() for c in upload_df.columns]

        self.state.upload_df = upload_df
        self.state.uploaded_file_name = uploaded_file.name

        # Auto-detect matching table
        matched_table = find_matching_table(upload_df.columns.tolist())
        if matched_table:
            self.state.auto_detected_table = matched_table['table']
            self.state.auto_detected_group = matched_table['group']
            if matched_table['match_type'] == 'exact':
                st.success(f"Auto-detected table: **{matched_table['table']}**", icon=":material/target:")
            else:
                st.info(f"Suggested table: **{matched_table['table']}** (partial match)", icon=":material/lightbulb:")
        else:
            self.state.auto_detected_table = None
            self.state.auto_detected_group = None
            st.warning("No matching table found. Please select manually.", icon=":material/warning:")

        st.write("Sample rows from file:")
        st.dataframe(data=upload_df.head(100), hide_index=True)
        return True


class TableSelectSection(Section):
    """Section 2: Table selection."""

    def _update_table_list(self):
        """Update table names when group changes."""
        if self.state.target_group_name:
            self.state.target_table_names = get_target_table_names(self.state.target_group_name)
        else:
            self.state.target_table_names = get_target_table_names()
        # Clear RefData when selection changes
        self.state.rd = None

    def render(self) -> bool:
        if self.state.upload_df is None:
            return False

        st.subheader("Select a table to write to")

        # Upload type selector
        upload_type_options = ['Merge (upsert)', 'Replace']
        current_upload_type_label = 'Replace' if self.state.upload_type == 'replace' else 'Merge (upsert)'
        upload_type_index = upload_type_options.index(current_upload_type_label)

        upload_type_select = st.selectbox(
            label="Upload type",
            options=upload_type_options,
            index=upload_type_index
        )
        new_upload_type = 'replace' if upload_type_select == 'Replace' else 'merge'
        if new_upload_type != self.state.upload_type:
            self.state.upload_type = new_upload_type
            self.state.rd = None

        # Group selector
        group_index = None
        if self.state.auto_detected_group and self.state.auto_detected_group in self.state.target_group_names:
            group_index = self.state.target_group_names.index(self.state.auto_detected_group)

        target_group_name = st.selectbox(
            label="Group",
            options=self.state.target_group_names,
            index=group_index,
            key='select_groups'
        )

        if target_group_name != self.state.target_group_name:
            self.state.target_group_name = target_group_name
            self._update_table_list()

        # Table selector
        table_index = None
        if self.state.auto_detected_table and self.state.auto_detected_table in self.state.target_table_names:
            table_index = self.state.target_table_names.index(self.state.auto_detected_table)

        target_table_name = st.selectbox(
            label="Table",
            options=self.state.target_table_names,
            index=table_index,
            key='select_table'
        )

        if target_table_name != self.state.target_table_name:
            self.state.target_table_name = target_table_name
            self.state.rd = None

        if self.state.target_table_name:
            st.write("Sample rows from table:")
            table_sample_df = get_target_sample(target_table=self.state.target_table_name)
            st.dataframe(data=table_sample_df, hide_index=True)
            return True

        return False


class ValidationSection(Section):
    """Section 3: Run validation (creates RefData)."""

    def render(self) -> bool:
        if self.state.upload_df is None or self.state.target_table_name is None:
            return False

        # Only run validation if we don't have a RefData object yet
        if self.state.rd is None:
            with st.spinner("Running checks..."):
                self.state.rd = RefData(
                    target=Target(target_table_name=self.state.target_table_name),
                    df=self.state.upload_df,
                    upload_type=self.state.upload_type,
                    ignore_schema_error=self.state.continue_on_schema_error
                )
        return True


class SchemaCheckSection(Section):
    """Section 4: Display schema check results."""

    def render(self) -> bool:
        if self.state.rd is None:
            return False

        st.subheader("Check schema")

        if self.state.rd.diffschema == []:
            st.info('Schema matches!', icon="✅")
            return True

        st.error('Schema of uploaded file differs to that of the target table.', icon="❌")
        st.write('The following columns were not found in the uploaded file:')
        st.dataframe(
            pd.DataFrame(self.state.rd.diffschema, columns=['Column name']),
            hide_index=True,
            use_container_width=True
        )

        if self.state.continue_on_schema_error:
            st.warning('Ignoring schema errors.', icon=':material/warning:')
            return True

        if st.button('Ignore and continue', type='primary', icon=":material/warning_off:"):
            self.state.continue_on_schema_error = True
            # Recreate RefData with ignore_schema_error=True
            self.state.rd = RefData(
                target=Target(target_table_name=self.state.target_table_name),
                df=self.state.upload_df,
                upload_type=self.state.upload_type,
                ignore_schema_error=True
            )
            st.rerun()

        return False


class DataTypeCheckSection(Section):
    """Section 5: Display data type check results."""

    def render(self) -> bool:
        if self.state.rd is None:
            return False

        st.subheader("Check data types")

        if self.state.rd.diffdatatypes == []:
            st.info('Data types match!', icon="✅")
            return True

        st.error('Data types of some uploaded values differ to that of the target table.', icon="❌")
        st.write('The following values were found in the uploaded file:')
        st.dataframe(
            pd.DataFrame(
                self.state.rd.diffdatatypes,
                columns=['Primary key', 'Column', 'Uploaded value', 'Expected Data type']
            ),
            hide_index=True,
            use_container_width=True
        )
        return False


class ImpactSection(Section):
    """Section 6: Show impact summary."""

    def render(self) -> bool:
        if self.state.rd is None:
            return False

        st.subheader('Impact')
        st.write('This upload will:')
        col1, col2 = st.columns([.5, .5])

        if self.state.upload_type == 'merge':
            col1.info(f"Insert {self.state.rd.impact['inserted']} rows.", icon=":material/add_circle:")
            col2.info(f"Update {self.state.rd.impact['updated']} rows.", icon=":material/sync:")
            self.state.has_changes = self.state.rd.impact['merge_rows_affected'] > 0
        else:
            col1.info(f"Delete {self.state.rd.impact['table_rows']} rows currently in the table.", icon=":material/delete_forever:")
            col2.info(f"Insert {self.state.rd.impact['upload_rows']} rows from the uploaded file.", icon=":material/add_circle:")
            self.state.has_changes = True

        if not self.state.has_changes:
            st.info('No changes to be uploaded. Data in file matches data already in table.', icon=':material/warning:')
            return False

        return True


class DQChecksSection(Section):
    """Section 7: Display DQ check results."""

    def render(self) -> bool:
        if self.state.rd is None or not self.state.has_changes:
            return False

        st.subheader("Data quality checks")

        for check_result in self.state.rd.check_results:
            if len(check_result.df) > 0:
                exp = st.expander(f"❌ **{check_result.check_type}**")
                exp.write(format_newline(f"{check_result.description}"))
                exp.info('Failed!', icon="❌")
                max_error_message = f"(first {MAX_ERROR_ROWS} rows of {len(check_result.df)})" if len(check_result.df) > MAX_ERROR_ROWS else ""
                exp.write(f"The **{check_result.check_type}** check has failed for the rows below{max_error_message}. Please correct the source file and upload again.")
                exp.dataframe(data=check_result.df[:MAX_ERROR_ROWS], hide_index=True)
            else:
                exp = st.expander(f"✅ **{check_result.check_type}**")
                exp.write(format_newline(f"{check_result.description}"))
                exp.info('Passed!', icon="✅")
                exp.write(f"The **{check_result.check_type}** check has passed for all rows.")

        return self.state.rd.all_checks_passed


class UploadSection(Section):
    """Section 8: Upload button and execution."""

    def render(self) -> bool:
        if self.state.rd is None or not self.state.rd.all_checks_passed:
            return False

        st.subheader("Upload data")

        if self.state.upload_complete:
            st.success("Upload complete!")
            if self.state.action_run:
                action = self.state.rd.target.action
                st.success(f"Action \"{action.get('name', 'Action')}\" executed.")
            return True

        action = self.state.rd.target.action
        run_action = True
        if action is not None and action.get('trigger') == 'optional':
            run_action = st.checkbox(
                label=action.get('name', 'Run action after upload'),
                value=True
            )

        if st.button("Upload", icon=":material/upload_file:"):
            with st.spinner("Uploading data..."):
                self.state.rd.upload_data()
            if action is not None and (action.get('trigger') == 'always' or run_action):
                with st.spinner(f"Running {action.get('name', 'action')}..."):
                    self.state.rd.run_action()
                self.state.action_run = True
            self.state.upload_complete = True
            st.rerun()

        return False


def main():
    st.title('RefDQ - Upload')

    # Load/create state
    state = AppState.from_session()

    # Header with reset button
    col1, col2 = st.columns([.87, .13])
    col1.write("Upload and validate reference data files.")
    if col2.button("Reset"):
        state.reset()
        state.save_to_session()
        st.rerun()

    # Define sections in order
    sections = [
        FileUploadSection(state),
        TableSelectSection(state),
        ValidationSection(state),
        SchemaCheckSection(state),
        DataTypeCheckSection(state),
        ImpactSection(state),
        DQChecksSection(state),
        UploadSection(state),
    ]

    # Render sections sequentially, stop at first incomplete
    for section in sections:
        is_complete = section.render()
        if not is_complete:
            break

    # Save state
    state.save_to_session()


if __name__ == "__main__":
    main()
