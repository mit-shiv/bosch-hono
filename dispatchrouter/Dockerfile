FROM fedora:latest

# installing needed libraries (i.e. for SASL, SSL/TLS, Python, ...)
RUN dnf -y install cyrus-sasl-lib cyrus-sasl-plain cyrus-sasl-sql libuuid openssl python
RUN dnf -y install qpid-proton-c qpid-dispatch-router qpid-dispatch-tools && \
    dnf -y update && \
    dnf clean all

CMD ["bash"]
