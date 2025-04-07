from django.shortcuts import render
from .utils.pdf_extraction import search_pdfs
from .utils.pdf_extraction import process_pdf_batch, urls, key_word_store, pdfs_key_words_store, titles
from .utils.pdf_extraction import titles
# Flag to only run once (you can improve this with a DB or cache later)

batch_size = 5
batches = [urls[i:i + batch_size] for i in range(0, len(urls), batch_size)]
for batch in batches:
    process_pdf_batch.delay(batch_urls=batch, titles=titles)
processing_started = True
print("Celery tasks dispatched for PDF preprocessing.")

def home(request):
    if request.method == 'POST':
        keywords_string = request.POST.get('keywords', '')
        keyword_list = [k.strip().lower() for k in keywords_string.split(',') if k.strip()]
        print("Submitted keywords:", keyword_list)

        if not keyword_list:
            return render(request, 'index.html', {'error': 'Please enter keywords.'})
        results = search_pdfs(keyword_list)

        return render(request, 'index.html', {
            'keywords_string': keywords_string,
            'keyword_list': keyword_list,
            'results': results
        })

    return render(request, 'index.html')
