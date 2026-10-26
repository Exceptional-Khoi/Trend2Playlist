import argparse
import csv
import io
import json
import os
import re
import shutil
import sys
import time

from .downloader import YoutubeCommentDownloader, SORT_BY_POPULAR, SORT_BY_RECENT

INDENT = 4


class CommentWriter:
    def __init__(self, output_file, file_format='jsonl', delimiter=','):
        self.output_file = output_file
        self.file_format = file_format
        self.delimiter = delimiter
        self.fp = None
        self.writer = None

    def open(self):
        is_csv_type = self.file_format in ('csv', 'scsv')
        encoding = 'utf-8-sig' if is_csv_type else 'utf8'
        newline = '' if is_csv_type else None
        self.fp = io.open(self.output_file, 'w', encoding=encoding, newline=newline)
        if self.file_format == 'json':
            self.fp.write('{\n' + ' ' * INDENT + '"comments": [\n')

    def write(self, comment, is_last=False):
        if self.file_format in ('csv', 'scsv'):
            if self.writer is None:
                self.writer = csv.DictWriter(
                    self.fp,
                    fieldnames=comment.keys(),
                    delimiter=self.delimiter,
                    quoting=csv.QUOTE_MINIMAL
                )
                self.writer.writeheader()
            self.writer.writerow(comment)
        else:
            # Format as single-line JSON or indented JSON
            indent = INDENT if self.file_format == 'json' else None
            comment_str = json.dumps(comment, ensure_ascii=False, indent=indent)

            if indent is not None:
                padding = ' ' * (2 * indent)
                comment_str = ''.join(padding + line for line in comment_str.splitlines(True))
                if not is_last:
                    comment_str += ','

            print(comment_str.decode('utf-8') if isinstance(comment_str, bytes) else comment_str, file=self.fp)

    def close(self):
        if self.fp:
            if self.file_format == 'json':
                self.fp.write(' ' * INDENT + ']\n}')
            self.fp.close()


def main(argv = None):
    parser = argparse.ArgumentParser(
        add_help=False,
        description='Download Youtube comments without using the Youtube API',
        formatter_class=lambda prog: argparse.HelpFormatter(
            prog,
            max_help_position=45,
            width=shutil.get_terminal_size((80, 20)).columns
        )
    )
    parser.add_argument('--help', '-h', action='help', default=argparse.SUPPRESS,
                        help='Show this help message and exit')
    parser.add_argument('--youtubeid', '-y', help='ID of Youtube video for which to download the comments')
    parser.add_argument('--url', '-u', help='Youtube URL for which to download the comments')
    parser.add_argument('--output', '-o', default=None, help='Output filename (optional, defaults to video_id.ext)')
    parser.add_argument(
        '--format', '-f',
        choices=['jsonl', 'json', 'csv', 'scsv'],
        default='jsonl',
        help='Output format: jsonl, json, csv (comma), or scsv (semicolon).'
    )
    parser.add_argument('--limit', '-l', type=int, help='Limit the number of comments')
    parser.add_argument('--language', '-a', type=str, default=None,
                        help='Language for Youtube generated text (e.g. en)')
    parser.add_argument('--sort', '-s', type=int, default=SORT_BY_RECENT,
                        help='Whether to download popular (0) or recent comments (1). Defaults to 1')

    try:
        args = parser.parse_args() if argv is None else parser.parse_args(argv)

        youtube_id = args.youtubeid
        youtube_url = args.url
        output = args.output
        limit = args.limit
        file_format = args.format

        if not youtube_id and not youtube_url:
            parser.print_usage()
            raise ValueError('you need to specify a Youtube ID or URL')

        if not output:
            # Fallback to using the video ID as filename if --output is omitted
            if youtube_id:
                extracted_id = youtube_id
            else:
                match = re.search(r'(?:v=|\/v\/|youtu\.be\/|\/embed\/|\/shorts\/)([a-zA-Z0-9_-]{11})', youtube_url)
                extracted_id = match.group(1) if match else 'youtube_comments'

            file_extension = 'csv' if file_format in ('csv', 'scsv') else file_format
            output = f"{extracted_id}.{file_extension}"

            if os.path.exists(output):
                raise FileExistsError(
                    f"The default output file '{output}' already exists. "
                    f"Use --output to specify a custom name or to overwrite the file."
                )

        if os.sep in output:
            outdir = os.path.dirname(output)
            if not os.path.exists(outdir):
                os.makedirs(outdir)

        print('Downloading Youtube comments for', youtube_id or youtube_url)
        downloader = YoutubeCommentDownloader()
        generator = (
            downloader.get_comments(youtube_id, args.sort, args.language)
            if youtube_id
            else downloader.get_comments_from_url(youtube_url, args.sort, args.language)
        )

        writer = None
        count = 0
        start_time = time.time()
        comment = next(generator, None)

        while comment:
            if not writer:
                delimiter = ';' if file_format == 'scsv' else ','
                writer = CommentWriter(output, file_format=file_format, delimiter=delimiter)
                writer.open()

            next_comment = None if limit and count + 1 >= limit else next(generator, None)

            writer.write(comment, is_last=(next_comment is None))
            count += 1
            sys.stdout.write('Downloaded %d comment(s)\r' % count)
            sys.stdout.flush()

            comment = next_comment

        if writer:
            writer.close()

        print('\n[{:.2f} seconds] Done!'.format(time.time() - start_time) if count else 'No comment available!')

    except Exception as e:
        print('Error:', str(e))
        sys.exit(1)
