# KYM's NSFW cover image is a node in the graph, standing in for 5,211 real images

**Status:** open (2026-10-02). Found while recomputing the IMKG comparison on
KG 6.5.0 (`KG_QUERIES.md`, "Findings worth acting on").

## What it is

On a page viewed without a login, KYM replaces every image it marks NSFW
with one cover, `https://a.kym-cdn.com/assets/image-covers/nsfw.png`. The
parser (1.7.0) reads the `<img src>`, so each covered image becomes that
cover, and the KG builds one `image` node for it.

Measured on 6.5.0 (2026-10-02):

- 2,553 frames link to the cover through `hasImage`, for 5,211 section
  images. By section: Various Examples 2,047, Spread 1,325, Origin 552.
- 3,125 events cite it through `eventImage`.
- In total 5,678 edges reach it. The next most-shared image in the graph
  has 20.
- It ranks sixth in PageRank over the whole graph, above every frame but
  YouTube, Reddit and Memes.

## Why it is fixable without scraping again

The real image is still in the stored page. A covered `<img>` carries the
original URL, base64-encoded, in two attributes:

```html
<img src="https://a.kym-cdn.com/assets/image-covers/nsfw.png" class="... nsfw-img"
     id="photo_2797016" title="Queen of Spades Cobson"
     data-nsfw-src="aHR0cHM6Ly9pLmt5bS1jZG4uY29tL3Bob3Rvcy9pbWFnZXMvbmV3c2ZlZWQv..."
     data-large-nsfw-src="aHR0cHM6Ly9pLmt5bS1jZG4uY29tL3Bob3Rvcy9pbWFnZXMvbmV3c2ZlZWQv...">
```

That one decodes to
`https://i.kym-cdn.com/photos/images/newsfeed/002/797/016/a44.png`. Pages
also contain a JavaScript template `<img class='nsfw-img'
data-data-nsfw-src='nsfw_masonry_image' ...>`, with placeholder values
instead of base64. It is not an image and must stay out.

## Why it matters

- Every analysis that ranks or counts images is skewed: the placeholder is
  a hub that links 2,553 unrelated frames.
- Image-level comparison or reuse (IMKG's image nodes, future visual work)
  sees one "image" where there are 5,211.
- The events that cite it point at a picture of the word "NSFW".

## What fixing looks like

1. **Parser 1.8.0.** For an `img.nsfw-img` whose `src` is the cover, take
   the URL from base64-decoding `data-nsfw-src` (or `data-large-nsfw-src`),
   and mark the image NSFW so the graph can carry the flag. Skip the
   template `<img>` with `data-data-*` attributes. Never emit the cover URL
   as an image.
2. **Re-parse from the stored pages** with `trigger_entities=false`: no
   scraping. Check that the event and entity staleness keys do not move,
   because the section text is unchanged.
3. **Rebuild the KG.** The cover node disappears, and 5,211 real image
   nodes take its place.
4. A test with a covered `<img>`, the template `<img>`, and an ordinary
   image.
