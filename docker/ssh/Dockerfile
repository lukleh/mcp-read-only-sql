FROM alpine:3.18

# Install SSH server and required tools
RUN apk add --no-cache openssh-server bash netcat-openbsd

# Create test user for SSH tunneling
RUN adduser -D -s /bin/bash tunnel && \
    echo "tunnel:tunnelpass" | chpasswd

# Generate SSH host keys
RUN ssh-keygen -A

# Configure SSH for tunneling
RUN sed -i 's/AllowTcpForwarding.*/AllowTcpForwarding yes/' /etc/ssh/sshd_config && \
    sed -i 's/#PermitTunnel.*/PermitTunnel yes/' /etc/ssh/sshd_config && \
    sed -i 's/#GatewayPorts.*/GatewayPorts yes/' /etc/ssh/sshd_config && \
    sed -i 's/#PasswordAuthentication.*/PasswordAuthentication yes/' /etc/ssh/sshd_config && \
    echo "GatewayPorts clientspecified" >> /etc/ssh/sshd_config && \
    echo "PermitRootLogin no" >> /etc/ssh/sshd_config && \
    echo "PubkeyAuthentication yes" >> /etc/ssh/sshd_config && \
    echo "TCPKeepAlive yes" >> /etc/ssh/sshd_config

# Create SSH directory for the tunnel user
RUN mkdir -p /home/tunnel/.ssh && \
    chown -R tunnel:tunnel /home/tunnel/.ssh && \
    chmod 700 /home/tunnel/.ssh

# Generate a test SSH key pair for key-based auth testing
RUN ssh-keygen -t rsa -b 2048 -f /home/tunnel/.ssh/test_key -N "" && \
    cat /home/tunnel/.ssh/test_key.pub >> /home/tunnel/.ssh/authorized_keys && \
    chmod 600 /home/tunnel/.ssh/authorized_keys && \
    chown tunnel:tunnel /home/tunnel/.ssh/*

# Copy the private key to a known location for tests to use
RUN cp /home/tunnel/.ssh/test_key /tmp/test_key && \
    chmod 644 /tmp/test_key

EXPOSE 22

# Start SSH daemon
CMD ["/usr/sbin/sshd", "-D", "-e"]