package com.s3.eca2.domain.counselType;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import javax.persistence.EntityNotFoundException;
import java.util.Date;

@Service
public class CounselTypeService {
    private final CounselTypeRepository counselTypeRepository;

    public CounselTypeService(CounselTypeRepository counselTypeRepository) {
        this.counselTypeRepository = counselTypeRepository;
    }

    public CounselType find(long counselTypeEid) {
        return counselTypeRepository.findById(counselTypeEid)
                .orElseThrow(() -> new EntityNotFoundException("OrganizationType is not found for id: " + counselTypeEid));
    }

    public Page<CounselType> findCounselTypeByDate(Date start, Date end, Pageable pageable) {
        return counselTypeRepository.findByRegDateBetweenOrModDateBetween(start, end, pageable);
    }

    public Page<CounselType> findAllCounselTypes(Pageable pageable) {
        return counselTypeRepository.findAll(pageable);
    }
}
