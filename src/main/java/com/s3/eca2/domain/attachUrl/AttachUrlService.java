package com.s3.eca2.domain.attachUrl;

import org.springframework.stereotype.Service;

import javax.persistence.EntityNotFoundException;
import java.util.Date;
import java.util.List;

@Service
public class AttachUrlService {

    private final AttachUrlRepository attachUrlRepository;

    public AttachUrlService(AttachUrlRepository attachUrlRepository) {
        this.attachUrlRepository = attachUrlRepository;
    }

    public AttachUrl find(long attachUrlEid) {
       return attachUrlRepository.findById(attachUrlEid)
                .orElseThrow(() -> new EntityNotFoundException("AttachUrl not found for id: " + attachUrlEid));
    }

    public List<AttachUrl> findAttachUrlByDate(Date start, Date end) {
        return attachUrlRepository.findByRegDateBetweenOrModDateBetween(start, end);
    }
}
