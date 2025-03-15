use chrono::Local;
use std::collections::HashMap;
use std::fmt::{self, Display};
use std::fs::File;
use std::io::Write;
use std::path::Path;

#[derive(Debug, Clone)]
pub struct IssueCollectorConfig {
    pub console_output: bool,
    pub emit_report: bool,
    pub report_path: String,
    pub stop_on_issue: bool,
    pub report_format: ReportFormat,
    pub current_context: DataContext,
}

#[derive(Debug, Clone)]
pub enum ReportFormat {
    Text,
    Json,
}

impl Default for IssueCollectorConfig {
    fn default() -> Self {
        Self {
            console_output: true,
            emit_report: true,
            report_path: "data_issues_report.txt".to_string(),
            stop_on_issue: false,
            report_format: ReportFormat::Text,
            current_context: DataContext::default(),
        }
    }
}

#[derive(Debug)]
pub struct IssueCollector {
    issues: Vec<Issue>,
    config: IssueCollectorConfig,
}

#[derive(Debug, Clone)]
pub struct Issue {
    context: DataContext,
    issue: String,
}

/// Context of the data that is being processed.
/// It is used to losely identify the source of the issue.
#[derive(Debug, Clone)]
pub struct DataContext {
    table: String,
    row: String,
}

impl Display for DataContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "In the {} table, row with {}.", self.table, self.row)
    }
}

impl Default for DataContext {
    fn default() -> Self {
        Self {
            table: "Undefined".to_string(),
            row: "Undefined".to_string(),
        }
    }
}

impl DataContext {
    pub fn new(table: String, row: String) -> Self {
        Self { table, row }
    }
}

impl IssueCollector {
    pub fn new(config: IssueCollectorConfig) -> Self {
        Self {
            issues: Vec::new(),
            config,
        }
    }

    pub fn with_default_config() -> Self {
        Self::new(IssueCollectorConfig::default())
    }

    pub fn set_context(&mut self, context: DataContext) {
        self.config.current_context = context;
    }

    // Report an issue and optionally stop execution
    pub fn report<D>(&mut self, issue: &str, default: D) -> D {
        let ctx = self.config.current_context.clone();
        if self.config.console_output {
            eprintln!("Data issue found: {} {}", ctx, issue);
        }

        if self.config.stop_on_issue {
            eprintln!("Validation failed. Configs have stop_on_issue set to true.");
            std::process::exit(1);
        }

        self.issues.push(Issue {
            context: ctx,
            issue: issue.to_string(),
        });

        default
    }

    pub fn report_with_context<D>(&mut self, issue: &str, context: DataContext, default: D) -> D {
        if self.config.console_output {
            eprintln!("Data issue found: {} {}", context, issue);
        }

        if self.config.stop_on_issue {
            std::process::exit(1);
        }

        self.issues.push(Issue {
            context,
            issue: issue.to_string(),
        });

        default
    }

    // Write report at the end of execution
    pub fn write_report(&self) -> std::io::Result<()> {
        if !self.config.emit_report || self.issues.is_empty() {
            return Ok(());
        }

        // Create directory if it doesn't exist
        if let Some(parent) = Path::new(&self.config.report_path).parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent)?;
            }
        }

        match self.config.report_format {
            ReportFormat::Text => self.write_text_report(&self.config.report_path),
            ReportFormat::Json => self.write_json_report(&self.config.report_path),
        }
    }

    fn write_text_report(&self, path: &str) -> std::io::Result<()> {
        let mut file = File::create(path)?;

        writeln!(file, "Data Validation Issues Report - {}", Local::now())?;
        writeln!(file, "=============================================")?;

        for (i, issue) in self.issues.iter().enumerate() {
            writeln!(
                file,
                "#{}: Context: {} Issue: {}",
                i + 1,
                issue.context,
                issue.issue
            )?;
        }

        writeln!(file, "\nTotal issues: {}", self.issues.len())?;

        Ok(())
    }

    fn write_json_report(&self, path: &str) -> std::io::Result<()> {
        let report_time = Local::now().to_string();
        let mut issues = Vec::new();

        for issue in &self.issues {
            let mut entry = HashMap::new();
            entry.insert("context", issue.context.to_string());
            entry.insert("issue", issue.issue.clone());
            issues.push(entry);
        }

        let report = HashMap::from([
            ("timestamp", report_time),
            ("total_issues", self.issues.len().to_string()),
        ]);

        let mut file = File::create(path)?;
        let json = serde_json::json!({
            "report_info": report,
            "issues": issues
        });

        file.write_all(serde_json::to_string_pretty(&json)?.as_bytes())?;

        Ok(())
    }
}

// Implementation for Drop to automatically write the report when the collector goes out of scope
impl Drop for IssueCollector {
    fn drop(&mut self) {
        if let Err(e) = self.write_report() {
            eprintln!("Failed to write issue report: {}", e);
        }
    }
}
