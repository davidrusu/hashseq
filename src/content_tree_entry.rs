enum Span {
    BeforeSpan {
	before: Id,
	content: String
    }
    AfterSpan {
	after: Id,
	content: String
    }
}

impl SplittableSpan for Span  {
}
