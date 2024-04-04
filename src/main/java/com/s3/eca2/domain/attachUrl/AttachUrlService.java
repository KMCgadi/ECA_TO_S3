package com.s3.eca2.domain.attachUrl;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import javax.persistence.EntityNotFoundException;
import java.util.Date;

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

    public Page<AttachUrl> findAttachUrlByDate(Date start, Date end, Pageable pageable) {
        return attachUrlRepository.findByRegDateBetweenOrModDateBetween(start, end, pageable);
    }
}
