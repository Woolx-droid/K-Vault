# syntax=docker/dockerfile:1.7
FROM nginx:1.27-alpine

COPY frontend/nginx.conf /etc/nginx/conf.d/default.conf

# Serve the same static pages used by Cloudflare Pages.
COPY index.html /usr/share/nginx/html/
COPY admin.html /usr/share/nginx/html/
COPY gallery.html /usr/share/nginx/html/
COPY webdav.html /usr/share/nginx/html/
COPY login.html /usr/share/nginx/html/
COPY preview.html /usr/share/nginx/html/
COPY block-img.html /usr/share/nginx/html/
COPY whitelist-on.html /usr/share/nginx/html/
COPY admin-imgtc.html /usr/share/nginx/html/
COPY admin-waterfall.html /usr/share/nginx/html/

COPY *.css /usr/share/nginx/html/
COPY *.js /usr/share/nginx/html/
COPY *.svg /usr/share/nginx/html/
COPY *.png /usr/share/nginx/html/
COPY *.ico /usr/share/nginx/html/
COPY _nuxt /usr/share/nginx/html/_nuxt

EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
