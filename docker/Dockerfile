# ---------------------------------------------------------------------
# base & build
# ---------------------------------------------------------------------
FROM python:3.10-alpine AS base

ARG TARGETARCH

WORKDIR /pyi
COPY . .
RUN apk update \
	&& apk add binutils
RUN pip3 install -r src/requirements.txt \
	&& pyinstaller main-${TARGETARCH}.spec

# ---------------------------------------------------------------------
# release
# ---------------------------------------------------------------------
FROM alpine:latest

WORKDIR /app
COPY --from=base /pyi/dist/main .
RUN ls -la /app
COPY src/static static
RUN chmod +x main \
	&& ls -la /app

CMD ["./main"]

#ENTRYPOINT ["tail", "-f", "/dev/null"]