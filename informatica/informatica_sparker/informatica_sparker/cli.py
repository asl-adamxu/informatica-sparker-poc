import argparse
import sys
import json
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(
        prog="informatica-sparker",
        description="Convert Informatica PowerCenter XML exports to PySpark code"
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    convert_parser = subparsers.add_parser("convert", help="Convert XML to PySpark code")
    convert_parser.add_argument("xml_file", help="Path to Informatica XML file")
    convert_parser.add_argument("-o", "--output", default="output", help="Output directory (default: output)")
    convert_parser.add_argument("-c", "--config", help="Path to user config YAML file")
    convert_parser.add_argument("--source-db", default="",
                              help="Override source database type (oracle, sqlserver, postgresql). Default: auto-detect from XML")
    convert_parser.add_argument("--target-db", default="",
                              help="Target database type for SQL translation (spark, oracle, sqlserver). Default: spark")
    convert_parser.add_argument("--with-tests", action="store_true",
                              help="Generate E2E test artifacts (schema DDL, reference data, pytest scripts)")

    analyze_parser = subparsers.add_parser("analyze", help="Analyze XML and show mapping details")
    analyze_parser.add_argument("xml_file", help="Path to Informatica XML file")
    analyze_parser.add_argument("--json", action="store_true", help="Output as JSON")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command == "convert":
        _run_convert(args)
    elif args.command == "analyze":
        _run_analyze(args)


def _run_convert(args):
    from .models import UserConfig
    from .service import ConversionService

    user_config = UserConfig()
    if args.config:
        user_config = _load_user_config(args.config)
    if args.source_db:
        user_config.source_db_type = args.source_db
    if args.target_db:
        user_config.target_db_type = args.target_db

    service = ConversionService(user_config=user_config)
    service.with_tests = args.with_tests

    try:
        result = service.convert_file(args.xml_file, output_dir=args.output)
    except (FileNotFoundError, ValueError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    print("=" * 60)
    print("  INFORMATICA-SPARKER CONVERSION COMPLETE")
    print("=" * 60)
    print(f"\n  Mappings Found:     {result.mapping_count}")
    print(f"  Mappings Converted: {result.mappings_processed}")
    print(f"  Files Generated:    {len(result.files)}")
    print(f"  SQL Queries Found:  {len(result.sql_queries)}")
    print(f"  Output Directory:   {args.output}/")

    print(f"\n  Generated Files:")
    py_files = [f for f in result.files if f.file_type == "python"]
    other_files = [f for f in result.files if f.file_type != "python"]

    if py_files:
        print(f"    Mapping Scripts:")
        for f in py_files:
            print(f"      - {f.filename}")

    if other_files:
        print(f"    Supporting Files:")
        for f in other_files:
            print(f"      - {f.filename} ({f.file_type})")

    if result.source_detections:
        print(f"\n  Source Detection:")
        for sd in result.source_detections:
            fmt_str = f" [{sd.file_format.value}]" if sd.file_format else ""
            print(f"    - {sd.source_name}: {sd.detected_type.value}{fmt_str}")
            for note in sd.detection_notes:
                print(f"        {note}")

    if result.errors:
        print(f"\n  ERRORS ({len(result.errors)}):")
        for e in result.errors:
            print(f"    ! {e}")

    if result.warnings:
        print(f"\n  Warnings ({len(result.warnings)}):")
        shown = result.warnings[:20]
        for w in shown:
            print(f"    ! {w}")
        if len(result.warnings) > 20:
            print(f"    ... and {len(result.warnings) - 20} more (see conversion_log.txt)")

    print()


def _run_analyze(args):
    from .service import ConversionService

    service = ConversionService()

    try:
        analysis = service.analyze_file(args.xml_file)
    except (FileNotFoundError, ValueError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    if getattr(args, 'json', False):
        print(json.dumps(analysis, indent=2, default=str))
    else:
        print(f"XML Type: {analysis['xml_type']}")
        print(f"Folder: {analysis.get('folder_name', 'N/A')}")
        print(f"Repository: {analysis.get('repository_name', 'N/A')}")
        print(f"Mappings: {analysis['mapping_count']}")

        for mapping in analysis.get('mappings', []):
            name = mapping.mapping_name if hasattr(mapping, 'mapping_name') else mapping.get('mapping_name', 'Unknown')
            print(f"\n  Mapping: {name}")

            sources = mapping.sources if hasattr(mapping, 'sources') else mapping.get('sources', [])
            targets = mapping.targets if hasattr(mapping, 'targets') else mapping.get('targets', [])
            print(f"    Sources: {len(sources)}")
            print(f"    Targets: {len(targets)}")

        if analysis.get('source_detections'):
            print(f"\n  Source Detection:")
            for sd in analysis['source_detections']:
                detected = sd.get('detected_type', 'UNKNOWN')
                fmt = sd.get('file_format')
                fmt_str = f" [{fmt}]" if fmt else ""
                print(f"    - {sd['source_name']}: {detected}{fmt_str}")
                for note in sd.get('detection_notes', []):
                    print(f"        {note}")

        wf = analysis.get('workflow_analysis', {})
        workflows = wf.get('workflows', [])
        if workflows:
            print(f"\nWorkflows: {len(workflows)}")
            for w in workflows:
                print(f"  - {w['name']}")


def _load_user_config(config_path: str):
    import yaml
    from .models import UserConfig, SourceConfig, TargetConfig

    path = Path(config_path)
    if not path.exists():
        print(f"Warning: Config file not found: {config_path}", file=sys.stderr)
        return UserConfig()

    with open(path, 'r') as f:
        data = yaml.safe_load(f) or {}

    config = UserConfig()

    if 'db_connections' in data:
        config.db_connections = data['db_connections']

    if 'connection_mappings' in data:
        config.connection_mappings = data['connection_mappings']

    if 'sources' in data:
        for src in data['sources']:
            config.sources.append(SourceConfig(**src))

    if 'targets' in data:
        for tgt in data['targets']:
            config.targets.append(TargetConfig(**tgt))

    return config


if __name__ == "__main__":
    main()
