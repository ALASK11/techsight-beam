"""Entry point for the TechSight Common Crawl script extraction pipeline."""

import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from techsight_beam.options import TechSightOptions
from techsight_beam.pipeline import build_pipeline


def run():
    logging.getLogger().setLevel(logging.INFO)

    pipeline_options = PipelineOptions()
    techsight_options = pipeline_options.view_as(TechSightOptions)

    with beam.Pipeline(options=pipeline_options) as p:
        build_pipeline(p, techsight_options)


if __name__ == "__main__":
    run()
