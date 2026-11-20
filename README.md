# Gen X assessment

Two independent applications, each with its own dependencies, setup instructions,
acceptance criteria, and implementation decisions:

- [FastAPI webhook assessment](fastapi-webhook-assessment/README.md): durable webhook
  processing, tenant-scoped idempotency, and bounded CPU preprocessing.
- [Next.js streaming assessment](nextjs-stream-assessment/README.md): server-rendered
  event loading, URL-backed filters, and cancellable summary streams.

Install and run each project from its own directory using its README. The frontend
uses its own mock data and does not require the FastAPI application.

The `.repro` directories preserve reproduction scripts and validation evidence.
Generated databases, environments, dependency directories, build output, and logs
are excluded from Git.
