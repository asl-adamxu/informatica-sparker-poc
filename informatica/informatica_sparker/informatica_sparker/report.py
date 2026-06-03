import json
from typing import List, Dict, Any
from .models import ConversionReport, ReportItem
from .ir import IRPlan


class ReportBuilder:

    QUALITY_GATES = [
        "df_source",
        "source_db",
        "lookup_db",
        "lookup_key",
        "$PM",
        "TODO:",
        "placeholder"
    ]

    def __init__(self, plan: IRPlan, generated_code: str):
        self.plan = plan
        self.generated_code = generated_code

    def build(self) -> ConversionReport:
        report = ConversionReport(
            mapping_name=self.plan.mapping_name,
            status="success"
        )

        for warning in self.plan.warnings:
            report.warnings.append(ReportItem(
                severity="warning",
                category="conversion",
                message=warning
            ))

        for error in self.plan.errors:
            report.errors.append(ReportItem(
                severity="error",
                category="conversion",
                message=error
            ))
            report.status = "partial"

        quality_issues = self._check_quality_gates()
        for issue in quality_issues:
            report.errors.append(issue)
            report.status = "needs_review"

        manual_items = self._identify_manual_items()
        report.manual_items = manual_items

        report.coverage_percent = self._calculate_coverage(report)

        return report

    def _check_quality_gates(self) -> List[ReportItem]:
        issues = []

        for gate in self.QUALITY_GATES:
            if gate in self.generated_code:
                count = self.generated_code.count(gate)
                issues.append(ReportItem(
                    severity="error",
                    category="quality_gate",
                    message=f"Found placeholder/unresolved reference: '{gate}' ({count} occurrences)",
                    details=f"The generated code contains '{gate}' which indicates an unresolved placeholder"
                ))

        return issues

    def _identify_manual_items(self) -> List[ReportItem]:
        manual_items = []

        for step in self.plan.steps:
            if step.warnings:
                for warning in step.warnings:
                    if "unsupported" in warning.lower() or "manual" in warning.lower():
                        manual_items.append(ReportItem(
                            severity="info",
                            category="manual_review",
                            message=warning,
                            details=f"Step: {step.step_name}"
                        ))

        if "TODO:" in self.generated_code:
            lines = self.generated_code.split('\n')
            for i, line in enumerate(lines):
                if "TODO:" in line:
                    manual_items.append(ReportItem(
                        severity="info",
                        category="todo",
                        message=line.strip(),
                        details=f"Line {i + 1}"
                    ))

        return manual_items

    def _calculate_coverage(self, report: ConversionReport) -> float:
        total_items = len(self.plan.steps) + 1

        issues = len(report.errors) + len(report.manual_items)

        coverage = max(0, (total_items - issues) / total_items * 100)

        return round(coverage, 1)

    def to_json(self) -> str:
        report = self.build()
        return json.dumps(report.model_dump(), indent=2)

    def to_dict(self) -> Dict[str, Any]:
        report = self.build()
        return report.model_dump()
