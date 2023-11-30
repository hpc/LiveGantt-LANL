FROM python:latest
LABEL authors="vhafener"

#ENTRYPOINT ["top", "-b"]

RUN cd /

RUN git clone https://gitlab+deploy-token-19:B2fRVwge9wz-RTGBWefe@gitlab.newmexicoconsortium.org/lanl-ccu/evalys.git
RUN python3 -m pip install ./evalys
RUN cd /

RUN git clone https://gitlab+deploy-token-20:MCPSeuLHzcxcnF4QTwBL@gitlab.newmexicoconsortium.org/lanl-ccu/batsimgantt.git
RUN python3 -m pip install ./batsimgantt
RUN cd /

RUN git clone https://gitlab+deploy-token-21:yDwbAFhh6zAEsXyWXoKG@gitlab.newmexicoconsortium.org/lanl-ccu/livegantt.git
RUN python3 -m pip install -r  ./livegantt/requirements.txt
RUN cd /


