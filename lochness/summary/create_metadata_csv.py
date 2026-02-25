"""Create a csv file with the metadata for site
under the PHOENIX/PROTECTED/site/{site}_metadata.csv
"""

import logging
from typing import Dict, Any
from pathlib import Path
from rich.logging import RichHandler
from lochness.models.files import File
from lochness.sources.redcap.tasks.pull_data import log_event
from lochness.helpers import utils, db, config


MODULE_NAME = "lochness.summary.create_metadata_csv"

console = utils.get_console()

logger = logging.getLogger(MODULE_NAME)
logargs: Dict[str, Any] = {
    "level": logging.DEBUG,
    "format": "%(message)s",
    "handlers": [RichHandler(rich_tracebacks=True)],
}
logging.basicConfig(**logargs)


def get_legacy_metadata_csv_for_mindlamp():
    """Get legacy metadata dataframe file for all site"""

    config_file = utils.get_config_file_path()
    query = """
        SELECT
            *
        FROM
            subjects s
        WHERE
            s.subject_metadata->>'missing_required_variables' IS NULL OR
            s.subject_metadata->>'missing_required_variables' = ''
        """

    df = db.execute_sql(
        config_file,
        query,
        db="postgresql"
    )

    df["Active"] = 1
    df["Consent"] = df.subject_metadata.apply(lambda x: x['consent_date'])

    df["Mindlamp"] = df.apply(
            lambda x: f"mindlamp.{x.project_id}{x.site_id}:"
            f"{x.subject_metadata['mindlamp_id']}",
            axis=1)

    df_to_save = df[["Active", "Consent", "project_id", "subject_id",
                     "site_id", "Mindlamp"]]

    df_to_save.columns = ["Active", "Consent", "Project", "Subject ID",
                          "Study", "Mindlamp"]
    return df_to_save


def write_legacy_metadata_csv_for_mindlamp():
    """Write legacy metadata csv file for all site"""
    config_file = utils.get_config_file_path()

    df = get_legacy_metadata_csv_for_mindlamp()

    for (project_id, site_id), project_site_df in df.groupby(
            ["Project", "Study"]):
        logger.info(f"Writing metadata for project {project_id} and "
                    f"site {site_id}")
        lochness_root: Path = config.parse(
                config_file, "general")["lochness_root"]  # type: ignore

        # Capitalize project name (first letter uppercase, rest lowercase)
        project_name_cap = (
            project_id[:1].upper() + project_id[1:].lower()
            if project_id
            else project_id
        )

        output_file = (
            Path(lochness_root)
            / project_name_cap
            / "PHOENIX"
            / "PROTECTED"
            / f"{project_name_cap}{site_id}"
            / f"{project_name_cap}{site_id}_metadata.csv"
            )

        project_site_df.to_csv(output_file, index=False)
        logger.info(f"Metadata for project {project_id} and site {site_id} "
                    f"written to {output_file}")

        # Record the file in the database
        file_model = File(
            file_path=output_file,
        )
        file_md5 = file_model.md5
        db.execute_queries(
            config_file,
            file_model.to_sql_queries_with_availability_update(),
            show_commands=False,
        )
        log_event(
            config_file=config_file,
            log_level="INFO",
            event="summary_create_metadata_csv",
            message=f"Successfully metadata for {project_id} to {site_id}.",
            project_id=project_id,
            site_id=site_id,
            extra={"file_path": str(output_file),
                   "file_md5": file_md5 if file_md5 else None},
        )


if __name__ == "__main__":
    write_legacy_metadata_csv_for_mindlamp()
