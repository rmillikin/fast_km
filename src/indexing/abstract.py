from indexing.km_util import sanitize_text

disallowed = ['\n', '\t', '\r']

class Abstract():
    def __init__(self, pmid: int, year: int, title: str, text: str, citation_count: int = 0):
        self.pmid = pmid
        self.pub_year = year
        self.original_title = title
        self.original_text = text
        self.title = sanitize_text(title)
        self.text = sanitize_text(text)
        self.citation_count = citation_count

        if not self.title or str.isspace(self.title):
            self.title = ' '
        if not self.text or str.isspace(self.text):
            self.text = ' '

    def to_dict(self) -> dict:
        return {
            'pmid': self.pmid,
            'pub_year': self.pub_year,
            'title': self.title,
            'text': self.text,
            'citation_count': self.citation_count,
        }