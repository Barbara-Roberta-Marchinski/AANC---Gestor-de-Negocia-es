#!/usr/bin/env python
"""
CLI script to run the DeepEval evaluation pipeline.
Designed for CI/CD integration but also usable locally.

Usage:
    python run_evaluation.py [--output-dir DIR] [--threshold THRESHOLD] [--verbose]
"""

import argparse
import json
import sys
from pathlib import Path

from src.deepeval_evaluation import run_golden_dataset_evaluation


def main():
    parser = argparse.ArgumentParser(
        description="Run the DeepEval evaluation pipeline on the golden dataset."
    )
    parser.add_argument(
        "--dataset-path",
        type=Path,
        default=Path("data/deepeval_golden_dataset.json"),
        help="Path to the golden dataset JSON file (default: data/deepeval_golden_dataset.json)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("evaluation_artifacts"),
        help="Directory to write evaluation reports and artifacts (default: evaluation_artifacts)",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.7,
        help="Score threshold for metric evaluation (default: 0.7)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose output during evaluation",
    )
    parser.add_argument(
        "--fail-on-regression",
        action="store_true",
        help="Exit with non-zero code if pass_rate falls below threshold or any case fails",
    )
    parser.add_argument(
        "--min-pass-rate",
        type=float,
        default=0.8,
        help="Minimum pass rate required (only used with --fail-on-regression, default: 0.8)",
    )

    args = parser.parse_args()

    if not args.dataset_path.exists():
        print(f"❌ Dataset not found: {args.dataset_path}", file=sys.stderr)
        return 1

    print(f"📊 Running DeepEval evaluation pipeline...")
    print(f"   Dataset: {args.dataset_path}")
    print(f"   Output directory: {args.output_dir}")
    print(f"   Threshold: {args.threshold}")
    print("-" * 80)

    try:
        results = run_golden_dataset_evaluation(
            args.dataset_path,
            output_dir=args.output_dir,
            threshold=args.threshold,
            verbose=args.verbose,
        )
    except Exception as e:
        print(f"❌ Evaluation failed with error: {e}", file=sys.stderr)
        return 1

    # Print summary
    print("\n" + "=" * 80)
    print("EVALUATION SUMMARY")
    print("=" * 80)
    print(f"Total Cases:     {results['total_cases']}")
    print(f"Passed Cases:    {results['passed_cases']}")
    print(f"Failed Cases:    {results['total_cases'] - results['passed_cases']}")
    print(f"Pass Rate:       {results['pass_rate']:.1%}")
    print("=" * 80)

    # Load and display summary if available
    summary_path = args.output_dir / "evaluation_summary.json"
    if summary_path.exists():
        with summary_path.open("r", encoding="utf-8") as f:
            summary = json.load(f)

        print("\nDetailed Metrics:")
        print(f"  Avg Relevancy Score:        {summary.get('average_scores', {}).get('relevancy_score', 'N/A')}")
        print(f"  Avg Faithfulness Score:     {summary.get('average_scores', {}).get('faithfulness_score', 'N/A')}")
        print(
            f"  Avg Contextual Precision:   {summary.get('average_scores', {}).get('contextual_precision_score', 'N/A')}"
        )
        print(
            f"  Avg Contextual Recall:      {summary.get('average_scores', {}).get('contextual_recall_score', 'N/A')}"
        )
        print(
            f"  Avg Contextual Relevancy:   {summary.get('average_scores', {}).get('contextual_relevancy_score', 'N/A')}"
        )

        if summary.get("best_question"):
            print(f"\n✅ Best Question:  {summary['best_question']['question'][:70]}...")
            print(f"   Score: {summary['best_question']['score']}")

        if summary.get("worst_question"):
            print(f"\n⚠️  Worst Question: {summary['worst_question']['question'][:70]}...")
            print(f"   Score: {summary['worst_question']['score']}")

    print(f"\n📁 Report artifacts written to: {args.output_dir}")
    print(f"   - evaluation_report.csv")
    print(f"   - evaluation_summary.json")

    # Check regression if requested
    if args.fail_on_regression:
        if results["pass_rate"] < args.min_pass_rate:
            print(
                f"\n❌ REGRESSION DETECTED: Pass rate {results['pass_rate']:.1%} below minimum {args.min_pass_rate:.1%}",
                file=sys.stderr,
            )
            return 1

        failed_cases = results["total_cases"] - results["passed_cases"]
        if failed_cases > 0:
            print(f"\n❌ {failed_cases} case(s) failed evaluation", file=sys.stderr)
            return 1

        print(f"\n✅ All checks passed!")

    return 0


if __name__ == "__main__":
    sys.exit(main())
