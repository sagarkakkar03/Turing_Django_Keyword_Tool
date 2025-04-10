from django.shortcuts import render
from django.http import JsonResponse
from .utils.pdf_extraction import search_pdfs
from .utils.pdf_extraction import process_pdf_batch, urls, key_word_store, pdfs_key_words_store, titles
from .utils.pdf_extraction import titles
# Flag to only run once (you can improve this with a DB or cache later)

def trigger_pdf_processing(request):
    if request.method in ['GET', 'POST']:  # Optional: restrict to POST for safety
        batch_size = 5
        batches = [urls[i:i + batch_size] for i in range(0, len(urls), batch_size)]
        for batch in batches:
            process_pdf_batch.delay(batch_urls=batch, titles=titles)

        return JsonResponse({'status': 'success', 'message': 'PDF processing tasks dispatched.'})
    return JsonResponse({'status': 'error', 'message': 'Invalid request method'}, status=405)

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
