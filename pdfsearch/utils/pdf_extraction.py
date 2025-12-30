from celery import shared_task
import io, requests
import fitz 
from collections import Counter
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.util import ngrams
from celery.utils.log import get_task_logger
from django.db import transaction, OperationalError
import time
from celery import chain
import nltk
try:
    stop_words = set(stopwords.words('english'))
except LookupError:
    nltk.download('stopwords')
    stop_words = set(stopwords.words('english'))

logger = get_task_logger(__name__)

urls = [
    'https://www.apple.com/newsroom/pdfs/The-Continued-Threat-to-Personal-Data-Key-Factors-Behind-the-2023-Increase.pdf',
    'https://www.itu.int/dms_pub/itu-s/opb/jnl/S-JNL-VOL2.ISSUE3-2021-A08-PDF-E.pdf',
    'https://lfedge.org/wp-content/uploads/sites/24/2020/07/LFedge_Whitepaper.pdf',
    'https://assets.aboutamazon.com/58/65/29ea168a4839979aa779700ba42c/aws-us-eis-2023.pdf',
    'https://www.internetsociety.org/wp-content/uploads/2021/11/Enablers-of-OGST-EN.pdf',
    'https://www.bis.org/publ/qtrpdf/r_qt2206b.pdf',
    'https://reutersinstitute.politics.ox.ac.uk/sites/default/files/2024-09/Simon%20et%20al%20Chatbots%20and%20UK%20Elections.pdf',
    'https://eprints.whiterose.ac.uk/185423/1/Teachers%20Use%20of%20Chatbots2022.pdf',
    'https://www.ncsc.gov.uk/files/Guidelines-for-secure-AI-system-development.pdf', 
]  

titles = {
    'https://www.apple.com/newsroom/pdfs/The-Continued-Threat-to-Personal-Data-Key-Factors-Behind-the-2023-Increase.pdf': 'The Continued Threat to Personal Data: Key Factors Behind the 2023 Increase',
    'https://www.itu.int/dms_pub/itu-s/opb/jnl/S-JNL-VOL2.ISSUE3-2021-A08-PDF-E.pdf': 'INTERNET OF BIO‑NANO THINGS: A REVIEW OF APPLICATIONS, ENABLING TECHNOLOGIES AND KEY CHALLENGES',
    'https://lfedge.org/wp-content/uploads/sites/24/2020/07/LFedge_Whitepaper.pdf': 'Sharpening the Edge: Overview of the LF Edge Taxonomy and Framework',
    'https://assets.aboutamazon.com/58/65/29ea168a4839979aa779700ba42c/aws-us-eis-2023.pdf': 'AWS Economic Impact Study',
    'https://www.internetsociety.org/wp-content/uploads/2021/11/Enablers-of-OGST-EN.pdf': 'Enablers of an Open, Globally Connected, Secure and Trustworthy Internet',
    'https://www.bis.org/publ/qtrpdf/r_qt2206b.pdf': 'The outsize role of cross-border financial centres',
    'https://reutersinstitute.politics.ox.ac.uk/sites/default/files/2024-09/Simon%20et%20al%20Chatbots%20and%20UK%20Elections.pdf': 'How Generative AI Chatbots Responded to Questions and Fact-checks about the 2024 UK General Election',
    'https://eprints.whiterose.ac.uk/185423/1/Teachers%20Use%20of%20Chatbots2022.pdf': 'Exploring the Role of Chatbots and Messaging Applications in Higher Education: A Teacher’s Perspective',
    'https://www.ncsc.gov.uk/files/Guidelines-for-secure-AI-system-development.pdf': 'Guidelines for secure AI system development'
}

stop_words = set(stopwords.words('english'))
custom_stopwords = {',', '(', ')', 'march', 'number', 'including', 'end', 'use'}
stop_words.update(custom_stopwords)

key_word_store = {}
pdfs_key_words_store = {}

@shared_task(bind=True, queue='io')
def fetch_pdf_text(self, url, title=None):
    logger.info(f"[I/O] Fetching PDF from {url}")

    try:
        response = requests.get(url, timeout=15)
        pdf_stream = io.BytesIO(response.content)
        doc = fitz.open(stream=pdf_stream, filetype="pdf")
        pdf_text = "".join([page.get_text() for page in doc])

        logger.info(f"[I/O] PDF fetched and parsed: {title[:50]}... ({len(pdf_text)} characters)")

        return {"url": url, "text": pdf_text, "title": title}
    except Exception as e:
        logger.error(f"Failed to fetch PDF from {url}: {e}")
        return None


@shared_task(bind=True, queue='cpu')
def extract_keywords_from_text(self, data):
    if not data:
        return None

    url = data["url"]
    pdf_text = data["text"]
    title = data["title"] or "Untitled"

    word_tokens = word_tokenize(pdf_text)
    filtered_sentence = [
        word.lower() for word in word_tokens
        if word.lower() not in stop_words and not word.isdigit()
    ]
    filtered_bigrams = [
        ' '.join(bigram)
        for bigram in ngrams(word_tokens, 2)
        if not any(word in stop_words or word.isdigit() for word in bigram)
    ]

    counts = Counter(filtered_sentence + filtered_bigrams)
    filtered_counts = {word: count for word, count in counts.items() if count >= 5}

    logger.info(f"[CPU] Processed: {title[:50]}... — {len(filtered_counts)} keywords extracted")

    # ✅ Return only essential info for next step
    return {
        "url": url,
        "title": title,
        "keywords": filtered_counts  # still needed by DB task
    }



@shared_task(bind=True, queue='db')
def store_keywords_in_db(self, data):
    if not data or not data.get("keywords"):
        logger.warning("No data to store in DB.")
        return

    url = data["url"]
    title = data["title"]
    keywords = data["keywords"]

    doc, _ = PDFDocument.objects.get_or_create(url=url, defaults={'title': title})
    word_list = list(keywords.keys())

    with transaction.atomic():
        existing_keywords = {
            k.word: k for k in Keyword.objects.filter(word__in=word_list)
        }

        new_keywords = [Keyword(word=word) for word in word_list if word not in existing_keywords]
        if new_keywords:
            Keyword.objects.bulk_create(new_keywords, ignore_conflicts=True)

        all_keywords = {
            k.word: k for k in Keyword.objects.filter(word__in=word_list)
        }

        freq_objs = [
            PDFKeywordFrequency(
                document=doc,
                keyword=all_keywords[word],
                count=count
            )
            for word, count in keywords.items() if word in all_keywords
        ]
        PDFKeywordFrequency.objects.bulk_create(freq_objs, ignore_conflicts=True)

    logger.info(f"[DB] Stored {len(freq_objs)} keywords for {url}")

from celery import chain

@shared_task(bind=True)
def process_pdf_batch(self, batch_urls, titles=None):
    logger.info(f"[BATCH] Processing new batch of {len(batch_urls)} URLs")
    start = time.time()
    for url in batch_urls:
        title = titles.get(url, 'Untitled') if titles else url
        chain(
            fetch_pdf_text.s(url, title),
            extract_keywords_from_text.s(),
            store_keywords_in_db.s()
        ).apply_async()
    finish = time.time() - start
    logger.info(f"Dispatched chained tasks for batch in {finish} seconds")
    return True





from ..models import PDFDocument, Keyword, PDFKeywordFrequency


from django.db.models import Count, Q, Prefetch

def search_pdfs(keywords):
    keywords = [word.lower() for word in keywords]
    keyword_count = len(keywords)

    # Step 1: Get the keyword objects
    keyword_qs = Keyword.objects.filter(word__in=keywords)
    keyword_ids = list(keyword_qs.values_list("id", flat=True))

    if len(keyword_ids) < keyword_count:
        return []  # Some keywords not found at all

    # Step 2: Find matching documents (that match ALL keywords)
    matched_doc_ids = (
        PDFKeywordFrequency.objects
        .filter(keyword_id__in=keyword_ids)
        .values("document_id")
        .annotate(match_count=Count("keyword_id", distinct=True))
        .filter(match_count=keyword_count)
        .values_list("document_id", flat=True)
    )

    if not matched_doc_ids:
        return []

    # Step 3: Fetch all required frequency rows in one query
    freqs = (
        PDFKeywordFrequency.objects
        .select_related("document", "keyword")
        .filter(document_id__in=matched_doc_ids, keyword_id__in=keyword_ids)
    )

    # Step 4: Build response
    doc_map = {}

    for freq in freqs:
        doc = freq.document
        keyword = freq.keyword.word

        if doc.id not in doc_map:
            doc_map[doc.id] = {
                "url": doc.url,
                "title": doc.title,
                **{k: 0 for k in keywords}  # initialize keyword counts
            }

        doc_map[doc.id][keyword] = freq.count

    return list(doc_map.values())


