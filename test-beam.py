import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# class to split a csv line by elements and keep only the columns we are interested in
def ParseCSVLineMapFn(line):
    Date, Open, High, Low, Close, Volume = line.split(",")
    return {
        'Date': Date,
        'Open': float(Open),
        'Close': float(Close)
    }

# setting input and output files
input_filename = "./data//sp500.csv"
output_filename = "/tmp/output/result.txt"

# instantiate the pipeline
options = PipelineOptions()

with beam.Pipeline(options=options) as p:
    # reading the csv and splitting lines by elements we want to retain
    csv_lines = (
        p
        | beam.io.ReadFromText(input_filename, skip_header_lines=1)
        | beam.Map(ParseCSVLineMapFn))

    # calculate the mean for Open values
    mean_open = (
        csv_lines
        | beam.Map(lambda x: (1, x['Open']))
        | beam.CombinePerKey(beam.combiners.MeanCombineFn()))

    # calculate the mean for Close values
    mean_close = (
        csv_lines
        | beam.Map(lambda x: (1, x['Close']))
        | beam.CombinePerKey(beam.combiners.MeanCombineFn()))

    # writing results to file
    output= (
        { 'Mean Open': mean_open, 'Mean Close': mean_close }
        | beam.CoGroupByKey()
        | beam.io.WriteToText(output_filename))
