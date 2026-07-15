# DeepEval CI/CD Integration

## Overview

This project integrates **DeepEval** evaluation into the CI/CD pipeline using GitHub Actions. The evaluation automatically runs on every push and pull request, generating detailed reports on chatbot performance.

## Workflow Configuration

### GitHub Actions Workflow: `.github/workflows/deepeval-evaluation.yml`

The workflow is triggered on:
- **Push** to `main` and `develop` branches
- **Pull Request** to `main` and `develop` branches
- **Manual trigger** via `workflow_dispatch`

#### Triggers:
The workflow monitors changes in:
- `src/**` - Source code changes
- `data/deepeval_golden_dataset.json` - Dataset updates
- `requirements.txt` - Dependency changes
- `test_deepeval_golden_dataset.py` - Test changes

### Workflow Jobs

#### 1. **Evaluation Job**
- Checks out code
- Sets up Python 3.13 with pip caching
- Installs dependencies
- Runs pytest tests: `test_deepeval_golden_dataset.py`
- Executes full DeepEval evaluation pipeline
- Uploads evaluation artifacts (CSV and JSON reports)
- Comments on PRs with a summary of results
- Fails if evaluation doesn't pass

#### 2. **Tests Job**
- Runs all unit tests with pytest
- Generates coverage reports
- Uploads coverage metrics to Codecov

## Local Usage

### Running Evaluation Locally

```bash
# Basic run with default settings
python run_evaluation.py

# With custom threshold
python run_evaluation.py --threshold 0.8

# Verbose output
python run_evaluation.py --verbose

# Custom output directory
python run_evaluation.py --output-dir my_reports/

# Fail if pass rate drops below threshold
python run_evaluation.py --fail-on-regression --min-pass-rate 0.9
```

### CLI Arguments

```
--dataset-path PATH           Path to golden dataset (default: data/deepeval_golden_dataset.json)
--output-dir DIR              Directory for reports (default: evaluation_artifacts)
--threshold SCORE             Metric threshold (default: 0.7)
--verbose                     Enable verbose output
--fail-on-regression          Exit with error if metrics regress
--min-pass-rate RATE          Minimum acceptable pass rate (default: 0.8)
```

## Artifacts

Each evaluation run generates:

### `evaluation_report.csv`
Detailed row-by-row results for each test case:
- Pergunta (question)
- Categoria (category)
- Dificuldade (difficulty)
- Resposta do Chatbot (actual response)
- Resposta Esperada (expected response)
- Contexto Recuperado (retrieved context)
- Score_relevancia (relevancy score)
- Score_faithfulness (faithfulness score)
- Score_contextual_precision
- Score_contextual_recall
- Score_contextual_relevancy
- Aprovado (pass/fail)
- Motivo_reprovacao (failure reason if applicable)

### `evaluation_summary.json`
High-level metrics and statistics:
```json
{
  "total_cases": 20,
  "approved_cases": 19,
  "rejected_cases": 1,
  "pass_rate": 0.95,
  "average_scores": {
    "relevancy_score": 0.92,
    "faithfulness_score": 0.88,
    "contextual_precision_score": 0.85,
    "contextual_recall_score": 0.90,
    "contextual_relevancy_score": 0.87
  },
  "best_question": {
    "question": "...",
    "score": 0.98
  },
  "worst_question": {
    "question": "...",
    "score": 0.72
  }
}
```

## CI/CD Features

### 1. **Automatic Comments on Pull Requests**
When a PR is evaluated, the workflow automatically posts a summary comment with:
- Total cases evaluated
- Pass rate
- Best/worst performing questions
- Link to full report artifacts

### 2. **Artifact Retention**
- Evaluation reports are stored for 30 days
- Downloadable from the Actions tab
- Can be compared across runs for trend analysis

### 3. **Environment Variables for CI**
- `GOOGLE_API_KEY` (optional) - For Gemini-based evaluation
  - If not set, falls back to MockJudge
  - Prevents sensitive credentials in logs

### 4. **Test Integration**
- Full pytest suite runs alongside evaluation
- Coverage metrics tracked
- Codecov integration (optional)

## Setup Instructions

### 1. Enable GitHub Actions
Ensure Actions are enabled in your repository settings.

### 2. (Optional) Configure Secrets
If using Gemini for evaluation:
```bash
# Add to repository secrets
Settings → Secrets and variables → Actions → New repository secret
Name: GOOGLE_API_KEY
Value: [your-key]
```

### 3. Monitor Results
- **Actions Tab**: View workflow runs and logs
- **PR Comments**: See summaries on pull requests
- **Artifacts**: Download reports from completed runs

## Monitoring & Metrics

### Key Metrics Tracked

1. **Relevancy Score**: Is the answer relevant to the question?
2. **Faithfulness Score**: Is the answer grounded in the retrieved context?
3. **Contextual Precision**: Is the retrieved context precise?
4. **Contextual Recall**: Does the context capture all relevant information?
5. **Contextual Relevancy**: Is the context relevant to the question?

### Pass Criteria

A test case is marked as "APPROVED" if:
- **All applicable metrics** score at or above the threshold (default: 0.7)
- At least one metric is evaluated successfully

### Regression Detection

Use `--fail-on-regression` flag to enforce minimum pass rate:
```bash
python run_evaluation.py --fail-on-regression --min-pass-rate 0.85
```

This ensures the CI/CD pipeline fails if:
- Overall pass_rate drops below the minimum
- Any individual test case fails

## Troubleshooting

### Workflow Fails to Trigger
- Verify file changes match the `paths` filter in the workflow
- Check if the branch matches `on.push.branches`

### Evaluation Skipped (No Context)
- Some metrics are skipped if no retrieval context is available
- This is normal and expected; the evaluation continues with applicable metrics

### API Key Issues
- The workflow gracefully falls back to MockJudge if API key is unavailable
- For production evaluation, ensure `GOOGLE_API_KEY` is set in repository secrets

### Large Evaluation Times
- If evaluating 20+ cases with Gemini, the workflow may take 5-10 minutes
- Timeout is set to 30 minutes; increase if needed in the workflow file

## Integration with Development Workflow

### Recommended Process

1. **Local Development**
   ```bash
   python run_evaluation.py --verbose
   ```

2. **Before Committing**
   ```bash
   python -m pytest test_deepeval_golden_dataset.py
   python run_evaluation.py --fail-on-regression
   ```

3. **After Pushing**
   - Check GitHub Actions for automated evaluation
   - Review PR comment with results
   - Download report artifacts if needed

### CI/CD Check Integration
The workflow can be used as a required status check:
- Settings → Branches → Branch protection rules
- Require "Run DeepEval Evaluation" to pass before merging

## Advanced: Customizing the Workflow

### Change Trigger Events
Edit `.github/workflows/deepeval-evaluation.yml` to trigger on different events:
```yaml
on:
  schedule:
    - cron: '0 2 * * *'  # Daily at 2 AM UTC
  issue_comment:
    types: [created]     # Trigger from PR comments
```

### Add Slack Notifications
```yaml
- name: Notify Slack
  if: always()
  uses: slackapi/slack-github-action@v1
  with:
    webhook-url: ${{ secrets.SLACK_WEBHOOK }}
```

### Store Results in Database
Extend `run_evaluation.py` to log results to a time-series database for trend analysis.

## Maintenance

### Update Golden Dataset
1. Edit `data/deepeval_golden_dataset.json`
2. Push changes
3. Workflow automatically re-evaluates with new cases
4. Review results in PR comment or artifacts

### Monitor Metric Trends
- Compare `average_scores` across workflow runs
- Identify regressions in chatbot quality
- Update thresholds if needed

---

**Next Steps:**
- Configure `GOOGLE_API_KEY` in repository secrets for production evaluation
- Set up branch protection rules to require evaluation to pass
- Monitor trends and adjust thresholds based on real performance data
