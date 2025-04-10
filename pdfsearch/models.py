from django.db import models


class PDFDocument(models.Model):
    url = models.URLField(unique=True)
    title = models.CharField(max_length=255)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return self.title or self.url

class Keyword(models.Model):
    word = models.CharField(max_length=100, unique=True)

    def __str__(self):
        return self.word

class PDFKeywordFrequency(models.Model):
    document = models.ForeignKey(PDFDocument, on_delete=models.CASCADE, related_name='frequencies')
    keyword = models.ForeignKey(Keyword, on_delete=models.CASCADE, related_name='frequencies')
    count = models.PositiveIntegerField()

    class Meta:
        unique_together = ('document', 'keyword')
