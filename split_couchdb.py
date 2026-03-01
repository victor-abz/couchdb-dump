#!/usr/bin/env python3
"""
Split and transform a raw CouchDB dump into importable chunks.

Transforms from raw _all_docs format:
  {"id":"...","key":"...","value":{"rev":"..."},"doc":{actual_doc}},

To bulk import format:
  {actual_doc},

Large documents (above threshold) get their own individual files.
Files are organized into subdirectories (max 10000 files per directory).
"""

import sys
import os
import re

MAX_FILES_PER_DIR = 10000

# Control characters that crash CouchDB's jiffy parser (0x00-0x1F except tab, newline, carriage return)
CONTROL_CHAR_PATTERN = re.compile(r'\\u00[01][0-9a-fA-F](?<!\\u0009)(?<!\\u000[aAdD])')

sanitized_count = 0

def sanitize_control_chars(text):
    """Remove Unicode control character escapes that crash CouchDB's jiffy parser."""
    global sanitized_count
    
    def replace_control(match):
        global sanitized_count
        char_code = int(match.group(0)[2:], 16)
        # Keep tab (9), newline (10), carriage return (13)
        if char_code in (9, 10, 13):
            return match.group(0)
        sanitized_count += 1
        return ''
    
    return re.sub(r'\\u00[01][0-9a-fA-F]', replace_control, text)

def get_suffix(n):
    """Convert number to 3-letter suffix (aaa, aab, ..., zzz)"""
    a = (n // 676) % 26
    b = (n // 26) % 26
    c = n % 26
    return chr(ord('a') + a) + chr(ord('a') + b) + chr(ord('a') + c)

def get_output_path(output_base, file_num):
    """Get output path with subdirectory organization."""
    dir_num = file_num // MAX_FILES_PER_DIR
    dir_name = f"{output_base}_dir{dir_num:04d}"
    
    # Create directory if needed
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)
    
    suffix = get_suffix(file_num)
    return os.path.join(dir_name, f"split{suffix}")

def transform_line(line):
    """Transform a raw CouchDB document line to bulk import format."""
    line = line.rstrip('\r\n')
    
    # Sanitize control characters that crash jiffy
    line = sanitize_control_chars(line)
    
    # Pattern: {"id":"...","key":"...","value":{"rev":"..."},"doc":{actual_doc}}
    match = re.match(r'^\{"id":"[^"]*","key":"[^"]*","value":\{"rev":"[^"]*"\},"doc":(.+)\}(,?)$', line)
    if match:
        doc = match.group(1)
        comma = match.group(2)
        return doc + comma
    
    return line

def write_batch_file(output_base, file_num, docs, header, footer):
    """Write a batch of documents to a file."""
    if not docs:
        return None
    
    out_path = get_output_path(output_base, file_num)
    
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(header + '\n')
        for i, doc in enumerate(docs):
            if i == len(docs) - 1:
                doc = doc.rstrip(',\n') + '\n'
            f.write(doc)
        f.write(footer + '\n')
    
    return out_path

def write_single_doc_file(output_base, file_num, doc, header, footer):
    """Write a single large document to its own file."""
    out_path = get_output_path(output_base, file_num)
    
    doc = doc.rstrip(',\n')
    
    with open(out_path, 'w', encoding='utf-8') as f:
        f.write(header + '\n')
        f.write(doc + '\n')
        f.write(footer + '\n')
    
    return out_path

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 split_couchdb.py <input_file> [docs_per_file] [output_base] [large_doc_threshold_kb]")
        print("  input_file: Raw CouchDB dump file")
        print("  docs_per_file: Documents per split file (default: 1000)")
        print("  output_base: Base name for output directories (default: input_file.split)")
        print("  large_doc_threshold_kb: Docs larger than this get their own file (default: 50)")
        print()
        print(f"Files are organized into subdirectories with max {MAX_FILES_PER_DIR} files each.")
        sys.exit(1)
    
    input_file = sys.argv[1]
    docs_per_file = int(sys.argv[2]) if len(sys.argv) > 2 else 1000
    output_base = sys.argv[3] if len(sys.argv) > 3 else input_file + ".split"
    large_threshold = int(sys.argv[4]) * 1024 if len(sys.argv) > 4 else 50 * 1024
    
    header = '{"new_edits":false,"docs":['
    footer = ']}'
    
    file_num = 0
    total_docs = 0
    total_large = 0
    batch_docs = []
    
    print(f"Processing {input_file}...")
    print(f"Docs per batch file: {docs_per_file}")
    print(f"Large doc threshold: {large_threshold // 1024}KB")
    print(f"Max files per directory: {MAX_FILES_PER_DIR}")
    print(f"Output base: {output_base}")
    print()
    
    with open(input_file, 'r', encoding='utf-8') as infile:
        for line_num, line in enumerate(infile, 1):
            if line_num == 1:
                if line.startswith('{"total_rows":') or line.startswith('{"new_edits":'):
                    print(f"Skipping header line: {line[:50]}...")
                    continue
            
            stripped = line.strip()
            if stripped == ']}':
                continue
            
            transformed = transform_line(line)
            if not transformed or transformed.strip() == '':
                continue
            
            doc_size = len(transformed.encode('utf-8'))
            total_docs += 1
            
            if doc_size > large_threshold:
                if batch_docs:
                    out_path = write_batch_file(output_base, file_num, batch_docs, header, footer)
                    print(f"  Wrote {out_path} ({len(batch_docs)} docs)")
                    file_num += 1
                    batch_docs = []
                
                out_path = write_single_doc_file(output_base, file_num, transformed, header, footer)
                print(f"  Wrote {out_path} (1 LARGE doc, {doc_size // 1024}KB)")
                file_num += 1
                total_large += 1
            else:
                batch_docs.append(transformed + '\n')
                
                if len(batch_docs) >= docs_per_file:
                    out_path = write_batch_file(output_base, file_num, batch_docs, header, footer)
                    print(f"  Wrote {out_path} ({len(batch_docs)} docs)")
                    file_num += 1
                    batch_docs = []
            
            if total_docs % 100000 == 0:
                print(f"  Processed {total_docs:,} documents ({total_large:,} large)...")
    
    if batch_docs:
        out_path = write_batch_file(output_base, file_num, batch_docs, header, footer)
        print(f"  Wrote {out_path} ({len(batch_docs)} docs)")
        file_num += 1
    
    num_dirs = (file_num // MAX_FILES_PER_DIR) + 1
    
    print()
    print(f"Done!")
    print(f"  Total documents: {total_docs:,}")
    print(f"  Large documents (individual files): {total_large:,}")
    print(f"  Total files created: {file_num:,}")
    print(f"  Directories created: {num_dirs}")
    if sanitized_count > 0:
        print(f"  Control chars sanitized: {sanitized_count:,} (would crash CouchDB)")
    print()
    print(f"To import, run:")
    print(f"  ./couchdb-dump.sh -i -n -H <host> -d <db> -f {output_base} -u <user> -p <pass> -c")

if __name__ == '__main__':
    main()
